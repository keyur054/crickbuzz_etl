import logging
import datetime
import logging
from airflow import models
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


bucket_path = "{{var.value.bucket_path}}"
project_id = "{{var.value.project_id}}"
gce_zone = "{{var.value.gce_zone}}"

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        # Set to your zone
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "tempLocation": bucket_path + "/tmp/",
    },
}

with models.DAG(
    # The id you will see in the DAG airflow page
    "cricbuzz_comoposer_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval= datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    def greetin():
        logging.info('Hello Airflow')

    def end():
        logging.info('End of flow')

    # def request_apidata():
    #     logging.info('Call api started')
    #     fetchCricketData.requestandConvertApiResponse()
    #     logging.info('Call api end')

    python_hello = PythonOperator(
        task_id = 'hello',
        python_callable = greetin
    )

    # call_api = PythonOperator(
    #     task_id = 'call_api_and_save_csv',
    #     python_callable = request_apidata
    # )

    # validate_team_file_exists = GCSObjectExistenceSensor(
    #     task_id = "validate_team_score_csv",
    #     bucket = bucket_path,
    #     object = "team_score.csv"
    # )

    # validate_innings_file_exists = GCSObjectExistenceSensor(
    #     task_id = "validate_innings_score_csv",
    #     bucket = bucket_path,
    #     object = "batsman_scorecard.csv"
    # )

    moveTeamScoreCSVToBigQuery = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='cricbuzz_score_csv',
        source_objects=['team_score.csv'],
        destination_project_dataset_table='cricbuzz-score.cricbuzz_stat.team_score',
        skip_leading_rows=1,
        schema_fields=[
    {
        "name": "runs",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "wickets",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "overs",
        "type": "FLOAT",
        "mode": "REQUIRED"
    },
    {
        "name": "runRate",
        "type": "FLOAT",
		"mode": "REQUIRED"
    },
    {
        "name": "team",
        "type": "STRING",
		"mode": "REQUIRED"
    },
    {
        "name": "teamL",
        "type": "STRING",
		"mode": "REQUIRED"
    },
	{
        "name": "inningsOrder",
        "type": "INTEGER",
		"mode": "REQUIRED"
    }]
            )

    movebatsmanScoreCSVToBigQuery = GCSToBigQueryOperator(
        task_id='load_batsmand_score_to_bq',
        bucket='cricbuzz_score_csv',
        source_objects=['batsman_scorecard.csv'],
        destination_project_dataset_table='cricbuzz-score.cricbuzz_stat.batsman_scorecard',
        skip_leading_rows=1,
        schema_fields=[
            {
                "name": "name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "strikeRate",
                "type": "FLOAT"
            },
            {
                "name": "team",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "runs",
                "type": "INTEGER"
            },
            {
                "name": "balls",
                "type": "INTEGER"
            },
            {
                "name": "fours",
                "type": "INTEGER"
            },
	            {
                "name": "sixes",
                "type": "INTEGER"
            },
	        {
                "name": "outDesc",
                "type": "STRING"
            }
            ])
   
    python_end = PythonOperator(
        task_id = 'end',
        python_callable = end
    )

    # python_hello >> call_api >> validate_team_file_exists >> validate_innings_file_exists >> python_end
    # python_hello >> validate_team_file_exists >> validate_innings_file_exists >> python_end
    python_hello >> moveTeamScoreCSVToBigQuery >> movebatsmanScoreCSVToBigQuery >> python_end
    # python_hello >> python_end