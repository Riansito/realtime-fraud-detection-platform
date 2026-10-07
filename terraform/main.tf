terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "0.7.1"
    }
  }
}

provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
}

resource "kafka_topic" "transactions" {
  name               = var.topic_transactions_name
  replication_factor = var.topic_transactions_replication_factor
  partitions         = var.topic_transactions_partitions

  config = {
    "retention.ms" = var.topic_transactions_retention_ms
  }
}

resource "kafka_topic" "fraud_alerts" {
  name               = var.topic_fraud_alerts_name
  replication_factor = var.topic_fraud_alerts_replication_factor
  partitions         = var.topic_fraud_alerts_partitions

  config = {
    "retention.ms" = var.topic_fraud_alerts_retention_ms
  }
}

resource "kafka_topic" "transactions_dlq" {
  name               = var.topic_dlq_name
  replication_factor = var.topic_dlq_replication_factor
  partitions         = var.topic_dlq_partitions

  config = {
    "retention.ms" = var.topic_dlq_retention_ms
  }
}
