FROM python:3.12-slim

# Install librdkafka-dev dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]
