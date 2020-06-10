#!/bin/bash

export AIRFLOW_HOME=~/data-warehouse-solution/airflow

FOLDER=$AIRFLOW_HOME/dags
FILE=batch_scheduler.py

# if [ ! -d $FOLDER ] ; then
#   sudo mkdir $FOLDER
#   sudo mv $FILE $FOLDER
# fi

cd $AIRFLOW_HOME
python3 $FOLDER/$FILE

airflow initdb
airflow webserver -p 8088 & disown
airflow scheduler & disown



