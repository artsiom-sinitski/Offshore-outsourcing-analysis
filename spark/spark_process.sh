#!/bin/bash

/usr/local/spark/bin/spark-submit --master spark://10.0.0.8:7077\
                                  --jars /home/ubuntu/usrlib/postgresql-42.2.9.jar\
                                  preprocess_save_data.py


