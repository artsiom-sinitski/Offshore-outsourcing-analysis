#!/bin/bash

FOLDER=$HOME/data-warehouse-solution/spark

# using positional parameter "$1"
case $1 in
    --manual)
        ARGS=manual
    ;;

    --schedule)
       ARGS=schedule
    ;;

    *)
        echo "Correct command usage: ./run_dw_etl.sh [--schedule | --manual]"
        exit 1
    ;;
esac

cd $FOLDER
# Run script to decompress and process files on EC2 spark cluster
/usr/local/spark/bin/spark-submit --master spark://10.0.0.8:7077 \
                                  --jars /home/ubuntu/usrlib/postgresql-42.2.9.jar \
                                  preprocess_save_to_central_storage.py $ARGS


