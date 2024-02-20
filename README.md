# Data Pipeline With Apache Airflow
## Background
The other day, I woke up and decided to learn Apache Airflow to add up my tech skills, so that's why this repo is here. #j4f
The actual reason is I found that it would be too inconvenient if I had to run and monitor some crawling tasks at a specific time each day. Therefore Airflow would be a nice choice with Workflow Management, Scheduling, Code-based Configuration, Monitoring and Debugging, bla..bla..bla...

## Overview
This repo is a data pipeline I built to extract the data from a dummy API that I found randomly on the Internet, save them into a CSV file, load them into a local Oracle Database, and then email me when everything is done or when there are errors during the ETL process. If the DAG did not run successfully on the first try for some reason, it will go for another try after some delay.

The process can be scheduled to run every day by setting the DAG schedule parameter, for example: by changing the preset from `schedule="@once"` to `schedule="@hourly"` the DAG will be executed once an hour at the beginning of the hour, or I can also set the DAG to run in a more specific time.

### docker-compose.yaml - Install Airflow
You have to install Docker then go to the terminal and run this line, it will create everything including image, containers and volumes we need to run the Airflow:
```
docker compose up
```
After everything is set up successfully, we can access the airflow platform through 
```
http://localhost:8080/home
```
Close everything
```
docker compose down
```
### dags folder - Pipelines folder
A DAG in Airflow is a collection of tasks that you want to run, organized in a way that reflects their relationships and dependencies.
In this folder, there are 2 .ipynb files, the etl_dag is the first one where I tried to save the CSV file to my local host machine but it seems that it needs further setup to be able to save the file from the docker container to local host machine. The second file is the one that save to DB and send email.
To run the DAGS, you can just open the Airflow platform, find the DAG in the list and click run

### data folder - Saved CSV
The CSV files which are created by the DAGs will be saved to this folder

### log folder - Logged files from the running process
These log files of each running time will greatly benefit if something is wrong when trying to run the pipeline, making it a lot easier to fix.

### plugins folder - Logged files from the running process
 The plugins folder in Airflow is where you can drop Python files to extend Airflow with custom operators, hooks, sensors, and more.

### config folder
The airflow.cfg file contains all the configuration options for Airflow. You can set these options in the airflow.cfg file or using environment variables.
