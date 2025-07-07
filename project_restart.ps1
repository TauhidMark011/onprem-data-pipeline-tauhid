Write-Host "Restarting On-Premise Data Pipeline Environment..."

# Step 1: Start Docker containers
docker-compose up -d --build
Start-Sleep -Seconds 20

# Step 2: Confirm core services are up
Write-Host "✅ Containers started. Waiting for MySQL, Kafka, Spark, Airflow to stabilize..."
Start-Sleep -Seconds 20

# Step 3: Insert fake devices into MySQL
Write-Host "Ensuring 'faker' Python module is available..."
docker exec -it airflow-webserver python -c "import faker" 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "'faker' not found. Installing..."
    docker exec -it airflow-webserver python -m pip install faker --user
    Start-Sleep -Seconds 5
} else {
    Write-Host "✅ 'faker' already installed."
}

Write-Host "Inserting Fake Devices into MySQL..."
docker exec -it airflow-webserver python /opt/airflow/kafka_producer/insert_fake_devices.py
Start-Sleep -Seconds 10

# Step 4: Run Weather Kafka Producer in background
Write-Host "Running Weather Kafka Producer in background..."
Start-Job -ScriptBlock {
    docker exec airflow-webserver python /opt/airflow/kafka_producer/weather_producer.py
}
Start-Sleep -Seconds 15

# Step 5: Run Spark Streaming (Kafka → Parquet)
Write-Host "Running Spark Streaming job...(Kafka → Parquet)"
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
  --master spark://spark-master:7077 `
  /opt/spark-app/streaming_kafka_to_parquet.py
Start-Sleep -Seconds 30

# Step 6: Run Spark Batch ETL job (Parquet + MySQL → Final Table)
Write-Host "Running Spark Batch ETL job..."
docker exec -it spark-master /opt/spark/bin/spark-submit --packages mysql:mysql-connector-java:8.0.33 --master spark://spark-master:7077 /opt/spark-app/spark_batch_etl.py
Start-Sleep -Seconds 20

# Step 7: Done
Write-Host "`n All components restarted and scripts executed."
Write-Host "Check MySQL: SELECT * FROM final_table;"
Write-Host "Open Airflow: http://localhost:8080"
Write-Host "Open Grafana: http://localhost:3000 (login: admin/admin)"
Write-Host "Prometheus validation: http://localhost:9090/targets"