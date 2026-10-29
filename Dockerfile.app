# One image for the three Python apps: DataGenerator, KafkaProducer (bridge), Dispatcher.
# score_alerts.py is inside too, so Airflow can run it with `docker exec generator ...`.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY DataGenerator.py KafkaProducer.py Dispatcher.py score_alerts.py ./

CMD ["python", "-u", "DataGenerator.py"]
