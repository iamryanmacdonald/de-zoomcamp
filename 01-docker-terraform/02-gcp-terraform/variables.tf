variable "project" {
  default     = "de-zoomcamp-414203"
  description = "Project ID"
}

variable "location" {
  default     = "australia-southeast1"
  description = "Project Location"
}

variable "bq_dataset_name" {
  default     = "demo_dataset"
  description = "Name for the BigQuery dataset"
}

variable "gcs_bucket_name" {
  default     = "de-zoomcamp-414203-terra-bucket"
  description = "Name for the Cloud Storage bucket"
}

variable "gcs_storage_class" {
  default     = "STANDARD"
  description = "Storage class for the Cloud Storage bucket"
}
