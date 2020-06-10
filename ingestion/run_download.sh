#!/bin/bash

FOLDER=$HOME/data-warehouse-solution/ingestion

# using positional parameter "$1"
case $1 in
    --manual)
        ARGS=manual
    ;;

    --schedule)
       ARGS=schedule
    ;;

    *)
        echo "Correct command usage: ./run_download.sh [--schedule | --manual]"
        exit 1
    ;;
esac

cd $FOLDER
# Run script to download raw data files form GDELT web site to AWS S3
python3 $FOLDER/download_files.py $ARGS