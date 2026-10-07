variable "topic_transactions_name" {
  description = "Nome do tópico de transações"
  type        = string
  default     = "transactions"
}

variable "topic_transactions_partitions" {
  description = "Número de partições para transactions"
  type        = number
  default     = 4
}

variable "topic_transactions_replication_factor" {
  description = "Fator de replicação para transactions"
  type        = number
  default     = 1
}

variable "topic_transactions_retention_ms" {
  description = "Retenção em milissegundos para transactions (7 dias)"
  type        = string
  default     = "604800000"
}

variable "topic_fraud_alerts_name" {
  description = "Nome do tópico de fraud_alerts"
  type        = string
  default     = "fraud_alerts"
}

variable "topic_fraud_alerts_partitions" {
  description = "Número de partições para fraud_alerts"
  type        = number
  default     = 2
}

variable "topic_fraud_alerts_replication_factor" {
  description = "Fator de replicação para fraud_alerts"
  type        = number
  default     = 1
}

variable "topic_fraud_alerts_retention_ms" {
  description = "Retenção em milissegundos para fraud_alerts (30 dias)"
  type        = string
  default     = "2592000000"
}

variable "topic_dlq_name" {
  description = "Nome do tópico de DLQ"
  type        = string
  default     = "transactions_dlq"
}

variable "topic_dlq_partitions" {
  description = "Número de partições para transactions_dlq"
  type        = number
  default     = 2
}

variable "topic_dlq_replication_factor" {
  description = "Fator de replicação para transactions_dlq"
  type        = number
  default     = 1
}

variable "topic_dlq_retention_ms" {
  description = "Retenção em milissegundos para transactions_dlq (14 dias)"
  type        = string
  default     = "1209600000"
}
