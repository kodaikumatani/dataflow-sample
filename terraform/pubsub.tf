resource "google_pubsub_topic" "location" {
  name = "location"

  message_retention_duration = "86400s" # 1 day
}

resource "google_pubsub_subscription" "location" {
  name  = "location-sub"
  topic = google_pubsub_topic.location.id

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  expiration_policy {
    ttl = "" # never expire
  }
}
