# 🛡️ Real-time Fraud Detection Platform

> Pipeline completo de Engenharia de Dados com arquitetura Lambda (Fast Path e Slow Path) para detecção de fraudes financeiras em tempo real, utilizando Mensageria, Streaming e um Data Warehouse modelado em estrela consumido por uma API analítica.

---

# 📖 Introdução

Atualmente, milhares de transações financeiras ocorrem a cada segundo. Detectar anomalias e possíveis fraudes em tempo real, enquanto se mantém um histórico analítico preciso, é um dos maiores desafios do mercado financeiro.

As informações transacionais costumam fluir em altíssimo volume através de eventos JSON, exigindo uma camada de processamento imediata para contenção de risco e uma camada histórica profunda para análise e business intelligence.

Este projeto resolve esse problema construindo um pipeline moderno de Engenharia de Dados (inspirado na arquitetura Medalhão e Lambda). Ele transforma dados de streaming brutos em alertas imediatos e, simultaneamente, alimenta um Data Warehouse organizado para consultas e dashboards táticos.

---

# 🎯 Problema de Negócio

No setor financeiro, a fraude deve ser identificada no momento em que acontece (sub-segundo). Porém, análises de perfil de risco e auditorias requerem dados históricos consistentes e dimensionalizados.

Esse cenário apresenta diversos desafios:

* Ingestão de alto volume em tempo real
* Deduplicação e tratamento de eventos atrasados (Late Data)
* Detecção imediata de anomalias (Heurísticas de velocidade e valores altos)
* Necessidade de conciliação de dados em batch
* Rastreabilidade de mudanças de perfil (SCD Type 2)

O objetivo deste projeto é fornecer uma plataforma dupla: um **Fast Path** para bloquear fraudes em tempo real e um **Slow Path** construindo um modelo dimensional para Analytics.

---

# 🚀 Objetivos

O projeto foi desenvolvido para demonstrar um pipeline moderno de Engenharia de Dados financeiro utilizando boas práticas do mercado.

Entre os principais objetivos estão:

* Gerar e ingerir transações sintéticas em streaming via Kafka
* Validar e limpar dados com Apache Spark (Bronze/Silver)
* Aplicar motor de regras em tempo real (Fast Path) e reenviar alertas
* Construir um Data Warehouse histórico utilizando dbt (Gold)
* Implementar SCD2 (Slowly Changing Dimensions) para rastrear o risco de contas/clientes
* Disponibilizar métricas operacionais e fila de triagem via FastAPI

---

# 🏗 Arquitetura

```text
               Python Producer
                      │
                      ▼
               Apache Kafka (Event Bus)
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
    Spark Streaming          Kafka Worker (Alerts)
 (Bronze & Fast Path)             │
          │                       │
          ▼                       ▼
 PostgreSQL (Silver)       PostgreSQL (Operational)
          │                       │
          ▼                       ▼
    Apache Airflow           FastAPI & UI
    (Orquestração)                │
          │                       │
          ▼                       ▼
       dbt Core          Dashboards & Triagem
     (Gold Layer)
```

Imagem da arquitetura da Plataforma:
<img width="1672" height="941" alt="b2f8b304-dd2b-461f-a83d-b2b25f3d41a7" src="https://github.com/user-attachments/assets/f50c6c92-f707-4bc1-8cf8-7a20a7a1c0cb" />


---

# ⚙ Tecnologias Utilizadas

### Linguagens

* Python
* SQL

### Streaming & Mensageria

* Apache Kafka
* Apache Spark (Structured Streaming)

### Orquestração

* Apache Airflow

### Transformação

* dbt (Data Build Tool)

### Banco de Dados & Storage

* PostgreSQL (Data Warehouse & Operacional)
* MinIO / Amazon S3 (Data Lake)

### Camada de Consumo / API

* FastAPI
* Uvicorn
* Pydantic
* HTMX (Frontend)

### Containers

* Docker
* Docker Compose

### Engenharia de Software & DevOps

* Pytest (Testes Integrados)
* Ruff (Linting & Formatação)
* GitHub Actions (CI/CD)

---

# 📂 Estrutura do Projeto

```text
realtime-fraud-detection-platform/

│
├── analytics/           # Projeto dbt (Gold Layer)
│   ├── models/
│   ├── snapshots/
│   └── seeds/
│
├── api/                 # FastAPI Backend e Triage UI
│   ├── main.py
│   └── worker.py
│
├── dags/                # Airflow DAGs
│   └── dbt_batch_dag.py
│
├── producer/            # Gerador de Transações (Python)
│
├── scripts/             # Scripts utilitários e Seeds gen
│
├── streaming/           # Spark Structured Streaming (Bronze/Silver)
│   ├── bronze_ingestion.py
│   └── silver_ingestion.py
│
├── tests/               # Pytest (API e Integração)
│
├── docker-compose.yml
└── README.md
```

---

# 📊 Modelagem Dimensional (Slow Path)

O projeto utiliza modelagem estrela (Star Schema) gerida pelo dbt.

## Fact

* `fact_transactions`

## Dimensões (Com SCD Type 2)

* `dim_customer` (Rastreia alterações no Perfil de Risco)
* `dim_account` (Rastreia Status de Bloqueio/Ativo)
* `dim_merchant`
* `dim_date`

---

# 🔄 Pipeline

## 1. Ingestão & Fonte (Producer)

Um processo Python simula compras em tempo real ao redor do mundo.
O JSON gerado é enviado imediatamente para o tópico `transactions` no Apache Kafka.

---

## 2. Processamento em Tempo Real (Fast Path)

O Apache Spark consome o streaming e realiza:

* Checkpointing da Bronze no S3.
* Schema Enforcement rigoroso (desvio para Dead Letter Queue no S3 caso inválido).
* Deduplicação (evita transações cobradas duas vezes via Watermarking de 10 min).
* Motor de Regras: Avalia velocidade, horários suspeitos e valores (Risk Score).
* Disparo Imediato: Se a pontuação passar de 70, o Spark posta a fraude no tópico `fraud_alerts`.

Ao final, os dados validados e higienizados vão para a tabela `silver.transactions` no DW.

---

## 3. Camada Analítica e Snapshots (Slow Path)

Periodicamente, o Apache Airflow orquestra o `dbt`:

* Tira "fotos" do estado atual dos clientes e contas para capturar mudanças ao longo do tempo (SCD Type 2).
* Transforma a tabela Silver em um poderoso Star Schema na Camada Gold, limpando duplicatas de rede finais e linkando Surrogate Keys.

---

## 4. Camada de Consumo e Operacional (API)

Enquanto o DW histórico é alimentado, uma API REST construída em FastAPI comuta a inteligência de negócios:

* Um Worker assíncrono consome os `fraud_alerts` em milissegundos e salva na tabela `operational.operational_alerts`.
* A API expõe a Fila de Triagem, Métricas Globais (Taxa de Fraude) e histórico de risco.

---

# 🛡️ Sentinel Risk Operations (UI)

O projeto possui uma interface em tempo real consumida pelos Analistas de Risco (desenvolvida com FastAPI e HTMX).

Na UI, o analista consegue visualizar:

1. Métricas principais (Taxa de Fraude global).
2. Valor financeiro total em risco.
3. Fila interativa de alertas críticos pendentes.
4. Botões de "Bloquear (Fraude)" ou "Aprovar (Seguro)" que processam as respostas no backend imediatamente.

<img width="1892" height="906" alt="Captura de tela 2026-10-08 152254" src="https://github.com/user-attachments/assets/3af57691-09ae-4969-b331-7badadec59ee" />

---

# ✅ Qualidade dos Dados

O pipeline conta com múltiplas barreiras de segurança:

* **Bronze/Silver**: Spark Asserts (Não deixa gravar no DW registros com moedas inválidas ou valores negativos). Dead Letter Queue (DLQ) absorve lixo sem quebrar a stream.
* **Gold**: dbt tests (Unicidade de surrogate keys, não-nulidade e integridade referencial nas Facts).

Todos os testes batem automaticamente.

---

# ▶ Como executar

## Clone o projeto

```bash
git clone https://github.com/Riansito/realtime-fraud-detection-platform.git
cd realtime-fraud-detection-platform
```

## Suba a Infraestrutura Completa (Kafka, Spark, Airflow, API, Postgres)

```bash
docker compose up -d --build
```

Isso iniciará o gerador de transações, o streaming do Spark, a API, e a infra de orquestração automaticamente!

## Acompanhe as Ferramentas

* **FastAPI Swagger**: `http://localhost:8000/docs`
* **Sentinel Risk UI (Triagem)**: `http://localhost:8000/ui`
* **Airflow Webserver**: `http://localhost:8080`
* **Kafka UI**: `http://localhost:8085`

---

# 📈 Melhorias Futuras

* Implementação de Machine Learning (Isolation Forests) no Spark para detecção não baseada em heurística.
* Cache de consultas em Redis para as métricas da API.
* Desacoplamento da Gold para rodar dentro do Snowflake ou Databricks.
* Deploy da API em Kubernetes (EKS/GKE).
* Grafana para monitoramento do throughput do Kafka.

---

# 👨💻 Autor

**Rian**

Estudante de Sistemas de Informação na UFPB, com foco em Engenharia de Dados e Analytics.

Este projeto foi desenvolvido com o objetivo de consolidar conhecimentos em pipelines de streaming em tempo real, arquitetura Lambda, dbt, modelagem dimensional e práticas modernas de Engenharia de Dados simulando um ambiente de missão crítica.
