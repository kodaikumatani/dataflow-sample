resource "google_bigtable_instance" "main" {
  name                = "dataflow-sample"
  deletion_protection = false

  cluster {
    cluster_id   = "dataflow-sample-c1"
    zone         = var.zone
    num_nodes    = 1
    storage_type = "SSD"
  }
}

resource "google_bigtable_table" "location" {
  name          = "location"
  instance_name = google_bigtable_instance.main.name

  column_family {
    family = "measurements"
  }
}

resource "google_bigtable_gc_policy" "measurements" {
  instance_name = google_bigtable_instance.main.name
  table         = google_bigtable_table.location.name
  column_family = "measurements"

  max_age {
    duration = "168h" # 7 days
  }
}
