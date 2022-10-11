# crickbuzz_etl
##Cricket Score Batch ETL pipeline (GCP).

An ETL pipeline that fetch cricket scores data for the specific match from a cricbuzz.com and transform in order to populate a team wise and innings wise score to data warehouse fro monitoring.


![ETL](https://user-images.githubusercontent.com/11287529/195139814-43bb4a9b-1336-465d-8a11-485f7f14bbce.jpg)


Schema for BigQuery

team_score

| Column Name | Type |
| ------------- | ------------- |
|runs |	INTEGER |
|wickets |	INTEGER |
|overs |	FLOAT |
|runRate |	FLOAT |
|team |	STRING |
|teamL |	STRING |
|inningsOrder |	STRING |


batsman_scorecard

| Column Name | Type |
| ------------- | ------------- |
| name |	STRING |
| strikeRate |	FLOAT |
| team |	STRING |
| runs |	INTEGER |
| balls |	INTEGER |
| fours |	INTEGER |
| sixes |	INTEGER |
| outDesc |	STRING |


**Key Scripts**

`cricbuzz_etl.py` Creates Apis request, fetch and transform data into CSV format inside Google Cloud Storage.

`cricbuzz_dag.py` Migrate CSV from cloud storage to Big Query.
