# How to run Spark ETL pipeline in Docker cluster mode

1. Build images and start cluster:
   docker-compose up --build

2. Access Spark Master UI: http://localhost:8080
   Access Worker UIs: http://localhost:8081 and http://localhost:8082

3. To run your ETL job, exec into master:
   docker exec -it spark-master bash
   # Example:
   /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/spark/batch_df_etl.py
   
4. Your scripts and data are mounted in /app/spark and /app/data

5. Stop cluster:
   docker-compose down

# Notes
- Python dependencies from req.txt are installed in all nodes.
- You can modify docker-compose.yml to add more workers if needed.
- For streaming jobs, use spark-submit with the appropriate script.
