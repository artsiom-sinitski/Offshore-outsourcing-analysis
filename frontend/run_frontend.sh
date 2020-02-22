  
#!/bin/bash

# Run this script to start a Flask web server for the Tableau dashboard
# Need to run it as super user, because port #'s below 1024 can only be run by root
nohup sudo python3 app.py &