terraform {
  required_providers {
    archive = {
      source = "hashicorp/archive"
      version = "2.4.0"
    }
    google = {
      source = "hashicorp/google"
      version = "4.73.1"
    }
  }
}

provider "google" {
  project = var.project_name
  region  = var.region
}

provider "archive" {}

resource "google_storage_bucket" "source_bucket" {
  name     = "mtg-pd-source-bucket"
  location = var.region
}

data "archive_file" "scraper_zip" {
  type          = "zip"
  source_dir    = "${path.module}/${var.scraper_folder}"
  output_path   = "${path.module}/scraper.zip"
}

resource "google_storage_bucket_object" "scraper_zip" {
  name   = "scraper${data.archive_file.scraper_zip.output_sha256}.zip"
  bucket = google_storage_bucket.source_bucket.name
  source = data.archive_file.scraper_zip.output_path
}

resource "google_pubsub_topic" "scraper_topic" {
  name = "mtg-pd-run-scraper"
}

resource "google_cloud_scheduler_job" "scraper_daily_job" {
  name        = "mtg-pd-run-scraper"
  description = "test job"
  schedule    = "0 12 * * *"
  time_zone   = "America/New_York"

  pubsub_target {
    topic_name = google_pubsub_topic.scraper_topic.id
    data       = base64encode("{}")
  }
}

resource "google_cloudfunctions_function" "scraper_cloud_function" {
  name        = "mtg-pd-api-scraper"
  description = "Scrapes Penny Dreadful Data"
  runtime     = "python39"
  available_memory_mb = 1024
  source_archive_bucket = google_storage_bucket.source_bucket.name
  source_archive_object = google_storage_bucket_object.scraper_zip.name
  entry_point = "entry_point"
  service_account_email = "foo-bar-sa@your-project-id.iam.gserviceaccount.com"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.scraper_topic.id
  }
}