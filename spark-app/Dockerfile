# spark-app/Dockerfile  (kept minimal — no extra Python deps required)
FROM apache/spark:3.5.1-python3
USER root
WORKDIR /opt/spark-app
COPY app.py ./
RUN chown -R spark:spark /opt/spark-app
USER spark
CMD ["python3","-c","print('Use docker compose to run ETL via spark-etl service')"]
