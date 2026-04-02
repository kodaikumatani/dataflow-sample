package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"sort"

	"cloud.google.com/go/cloudsqlconn"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/golang/geo/s2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const earthRadiusMeters = 6371000

func init() {
	register.DoFn3x1(&batchWriteFn{})
}

// batchWriteFn processes location updates grouped by ID and updates total_distance.
type batchWriteFn struct {
	Instance string
	DBName   string
	User     string
	Password string

	db     *sql.DB
	dialer *cloudsqlconn.Dialer
}

func (fn *batchWriteFn) Setup(ctx context.Context) error {
	dialer, err := cloudsqlconn.NewDialer(ctx)
	if err != nil {
		return fmt.Errorf("cloudsqlconn.NewDialer: %w", err)
	}
	fn.dialer = dialer

	dsn := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", fn.User, fn.Password, fn.DBName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("pgx.ParseConfig: %w", err)
	}

	instanceName := fn.Instance
	config.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.Dial(ctx, instanceName)
	}

	dbURI := stdlib.RegisterConnConfig(config)
	db, err := sql.Open("pgx", dbURI)
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	fn.db = db
	return nil
}

func (fn *batchWriteFn) ProcessElement(ctx context.Context, id string, iter func(*Location) bool) error {
	// Collect and sort locations by timestamp.
	var locs []Location
	var loc Location
	for iter(&loc) {
		locs = append(locs, loc)
	}
	if len(locs) == 0 {
		return nil
	}

	sort.Slice(locs, func(i, j int) bool {
		return locs[i].Timestamp < locs[j].Timestamp
	})

	// Calculate intra-batch distance.
	var intraDistance float64
	for i := 1; i < len(locs); i++ {
		p1 := s2.PointFromLatLng(s2.LatLngFromDegrees(locs[i-1].Lat, locs[i-1].Lng))
		p2 := s2.PointFromLatLng(s2.LatLngFromDegrees(locs[i].Lat, locs[i].Lng))
		intraDistance += p1.Distance(p2).Radians() * earthRadiusMeters
	}

	first := locs[0]
	last := locs[len(locs)-1]

	// Single UPSERT: gap distance (DB → first point) calculated in SQL via PostGIS.
	_, err := fn.db.ExecContext(ctx, `
		INSERT INTO telemetry (id, total_distance, location)
		VALUES ($1, $2, ST_SetSRID(ST_Point($3, $4), 4326))
		ON CONFLICT (id) DO UPDATE SET
			total_distance = telemetry.total_distance
				+ ST_Distance(
					telemetry.location::geography,
					ST_SetSRID(ST_Point($5, $6), 4326)::geography
				)
				+ $2,
			location = ST_SetSRID(ST_Point($3, $4), 4326)
	`, id, intraDistance, last.Lng, last.Lat, first.Lng, first.Lat)
	if err != nil {
		return fmt.Errorf("upsert: %w", err)
	}

	log.Printf("Updated id=%s intra_distance=%.2f", id, intraDistance)

	return nil
}

func (fn *batchWriteFn) Teardown() error {
	if fn.db != nil {
		fn.db.Close()
	}
	if fn.dialer != nil {
		fn.dialer.Close()
	}
	return nil
}
