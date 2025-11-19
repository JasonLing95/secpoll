# Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY get_logging.py utils.py main.py models.py /app/
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs

ENV EDGAR_USE_RICH_LOGGING="1"

ENTRYPOINT ["python", "main.py"]
CMD ["--cik", "/app/ciks.txt"]