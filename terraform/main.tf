variable "project_id" {
  type = string
}

variable "region" {
  default = "us-central1"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "spark_batch" {
  name          = "${var.project_id}-spark-batch"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "nyc_taxi" {
  dataset_id = "nyc_taxi_analytics"
  location   = var.region
}

resource "google_dataproc_cluster" "spark_cluster" {
  name   = "taxi-spark-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
    }
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-4"
    }
    software_config {
      image_version = "2.1-debian11"
    }
  }
}

output "bucket_name" {
  value = google_storage_bucket.spark_batch.name
}

output "cluster_name" {
  value = google_dataproc_cluster.spark_cluster.name
}
