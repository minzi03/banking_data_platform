FROM python:3.11-slim

WORKDIR /app

COPY freshness_exporter.py .

EXPOSE 9119

CMD ["python", "-u", "freshness_exporter.py"]
