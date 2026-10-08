"""
Gera os seeds do dbt (analytics/seeds/*.csv) cobrindo exatamente os ranges de IDs
usados pelo producer (producer/main.py), para que toda transação encontre suas dimensões.

    CUST-100..999     -> raw_customers.csv
    ACC-1000..9999    -> raw_accounts.csv
    MERCH-1000..9999  -> raw_merchants.csv

Determinístico (seed fixa): rodar de novo gera os mesmos arquivos.
Uso: python scripts/generate_seeds.py
"""

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEEDS_DIR = Path(__file__).resolve().parent.parent / "analytics" / "seeds"
rng = random.Random(42)

FIRST = ["João", "Maria", "Carlos", "Ana", "Lucas", "Juliana", "Pedro", "Fernanda",
         "Rafael", "Camila", "Bruno", "Larissa", "Gabriel", "Beatriz", "Mateus", "Paula"]
LAST = ["Silva", "Oliveira", "Santos", "Costa", "Pereira", "Souza", "Lima", "Almeida",
        "Ferreira", "Rodrigues", "Gomes", "Martins", "Araújo", "Ribeiro", "Carvalho"]
CITIES = [("São Paulo", "SP", "11"), ("Rio de Janeiro", "RJ", "21"),
          ("Belo Horizonte", "MG", "31"), ("Curitiba", "PR", "41"),
          ("Porto Alegre", "RS", "51"), ("Salvador", "BA", "71"),
          ("Recife", "PE", "81"), ("Brasília", "DF", "61")]
# Mesmas categorias do producer
CATEGORIES = ["RETAIL", "FOOD", "ELECTRONICS", "TRAVEL", "ENTERTAINMENT", "HEALTH", "SERVICES"]
MERCHANT_WORDS = ["Central", "Prime", "Express", "Global", "Nova", "Max", "Top", "Plus"]
COUNTRIES = ["BR", "BR", "BR", "US", "UK", "FR"]


def strip_accents(s: str) -> str:
    return s.translate(str.maketrans("ãáâéêíóôõúçÁÂÃÉÍÓÚÇ", "aaaeeiooouc" + "AAAEIOUC"))


def write(name, header, rows):
    with open(SEEDS_DIR / name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"{name}: {len(rows)} linhas")


def main():
    customer_ids = [f"CUST-{n}" for n in range(100, 1000)]

    customers = []
    for cid in customer_ids:
        first, last = rng.choice(FIRST), rng.choice(LAST)
        city, state, ddd = rng.choice(CITIES)
        email = f"{strip_accents(first).lower()}.{strip_accents(last).lower()}{cid[5:]}@email.com"
        phone = f"{ddd}9{rng.randint(10000000, 99999999)}"
        risk = rng.choices(["LOW", "MEDIUM", "HIGH"], weights=[70, 22, 8])[0]
        customers.append([cid, f"{first} {last}", email, phone, risk, city, state])
    write("raw_customers.csv",
          ["customer_id", "full_name", "email", "phone", "risk_profile", "city", "state"],
          customers)

    accounts = []
    for n in range(1000, 10000):
        acc_type = rng.choices(["CREDIT", "DEBIT"], weights=[65, 35])[0]
        limit = f"{rng.choice([1000, 2500, 5000, 10000, 15000, 25000]):.2f}" if acc_type == "CREDIT" else "0.00"
        status = rng.choices(["ACTIVE", "BLOCKED"], weights=[97, 3])[0]
        accounts.append([f"ACC-{n}", rng.choice(customer_ids), acc_type, limit, status])
    write("raw_accounts.csv",
          ["account_id", "customer_id", "account_type", "credit_limit", "account_status"],
          accounts)

    merchants = []
    base = datetime(2018, 1, 1, tzinfo=timezone.utc)
    for n in range(1000, 10000):
        cat = CATEGORIES[n % len(CATEGORIES)]
        name = f"{cat.title()} {rng.choice(MERCHANT_WORDS)} {n}"
        created = base + timedelta(minutes=rng.randint(0, 6 * 365 * 24 * 60))
        merchants.append([f"MERCH-{n}", name, cat.lower(), rng.choice(COUNTRIES),
                          created.strftime("%Y-%m-%d %H:%M:%S")])
    write("raw_merchants.csv",
          ["merchant_id", "merchant_name", "category", "country", "created_at"],
          merchants)


if __name__ == "__main__":
    main()
