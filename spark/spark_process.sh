#!/bin/bash

/usr/local/spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.9 --jars /home/ubuntu/data-warehouse-solution/usrlib/postgresql-42.2.9.jar preprocess_store_data.py


