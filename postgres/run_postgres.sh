#!/bin/bash

# Run this script to start a postgres server for the web API
sudo service postgresql start;
sudo -u postgres -i psql 
