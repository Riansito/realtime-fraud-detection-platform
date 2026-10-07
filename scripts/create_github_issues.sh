#!/bin/bash
issues=(
  "ISSUE-03 - Spark Ingestion & Bronze Layer|Gerenciamento de atividades relacionadas à Issue 03"
  "ISSUE-04 - Streaming Core: Silver & DLQ|Gerenciamento de atividades relacionadas à Issue 04"
  "ISSUE-05 - Fast Path: Motor de Risco e Alertas|Gerenciamento de atividades relacionadas à Issue 05"
  "ISSUE-06 - Slow Path: Camada Gold com dbt (SCD 2)|Gerenciamento de atividades relacionadas à Issue 06"
  "ISSUE-07 - Camada de Consumo: FastAPI & UI|Gerenciamento de atividades relacionadas à Issue 07"
  "ISSUE-08 - Qualidade de Dados & Observabilidade|Gerenciamento de atividades relacionadas à Issue 08"
  "ISSUE-09 - CI/CD & Automação Completa|Gerenciamento de atividades relacionadas à Issue 09"
)

for item in "${issues[@]}"; do
    title="${item%%|*}"
    body="${item##*|}"
    echo "Creating $title..."
    gh issue create --title "$title" --body "$body"
    sleep 1
done
