from google.cloud import bigquery
from datetime import datetime, timezone
import time
import logging


class LockTable:

    def __init__(self):
        self.client = bigquery.Client()

    def get_lock_table_data(self):
        query = """
        SELECT *
        FROM `cloud-db-nl2sql.evalbench.lock`
        """

        client = self.client
        query_job = client.query(query)
        results = query_job.result()

        return results

    def get_available_databases(self, dialect, num_required_databases, timeout_minutes=20):
        client = self.client
        wait_time = min(5, timeout_minutes) * 60

        while True:
            now = datetime.now(timezone.utc).replace(microsecond=0)

            # Fetch all available databases
            fetch_query = """
            SELECT db_name
            FROM `cloud-db-nl2sql.evalbench.lock`
            WHERE (lock_time IS NULL OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(lock_time), MINUTE) > @timeout)
            AND dialect = @dialect
            """

            fetch_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("timeout", "INT64", timeout_minutes),
                    bigquery.ScalarQueryParameter("dialect", "STRING", dialect)
                ]
            )

            fetch_query_job = client.query(fetch_query, job_config=fetch_job_config)
            fetch_results = fetch_query_job.result()
            available_dbs = [row['db_name'] for row in fetch_results]

            # Check if the required number of databases is available
            if len(available_dbs) >= num_required_databases:
                dbs_to_lock = available_dbs[:num_required_databases]

                # Lock the required databases
                lock_query = """
                UPDATE `cloud-db-nl2sql.evalbench.lock`
                SET lock_time = @now
                WHERE db_name IN UNNEST(@db_names)
                AND dialect = @dialect
                """

                lock_job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("db_names", "STRING", dbs_to_lock),
                        bigquery.ScalarQueryParameter("dialect", "STRING", dialect),
                        bigquery.ScalarQueryParameter("now", "TIMESTAMP", now)
                    ]
                )

                lock_query_job = client.query(lock_query, job_config=lock_job_config)
                lock_query_job.result()  # Wait for the job to complete

                return dbs_to_lock

            # Wait for min(5 minutes, timeout_minutes) before retrying
            logging.info(f"Not enough databases available. Retrying in {wait_time // 60} minutes...")
            time.sleep(wait_time)

    def insert_lock_data(self, db_names, dialect):
        query_template = """
        MERGE `cloud-db-nl2sql.evalbench.lock` T
        USING (SELECT db_name, @dialect AS dialect FROM UNNEST(@db_names) AS db_name) S
        ON T.db_name = S.db_name AND T.dialect = S.dialect
        WHEN MATCHED THEN
        UPDATE SET T.lock_time = NULL
        WHEN NOT MATCHED THEN
        INSERT (db_name, lock_time, dialect)
        VALUES (S.db_name, NULL, S.dialect)
        """

        client = self.client
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("db_names", "STRING", db_names),
                bigquery.ScalarQueryParameter("dialect", "STRING", dialect)
            ]
        )

        query_job = client.query(query_template, job_config=job_config)
        query_job.result()

    def release_databases(self, databases, dialect):
        query = """
        UPDATE `cloud-db-nl2sql.evalbench.lock`
        SET lock_time = NULL
        WHERE db_name IN UNNEST(@databases)
        AND dialect = @dialect
        """

        client = self.client
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("databases", "STRING", databases),
                bigquery.ScalarQueryParameter("dialect", "STRING", dialect)
            ]
        )

        query_job = client.query(query, job_config=job_config)
        query_job.result()
