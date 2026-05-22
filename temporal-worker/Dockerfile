FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libsnappy-dev \
    libsnappy1v5 \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir python-snappy==0.7.2

RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import snappy; print('Snappy OK')"

CMD ["python", "worker.py"]
