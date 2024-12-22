#!/bin/bash

if [[ $# -eq 0 ]]; then
    input=""
else
    if ! [[ $1 =~ ^[1-9][0-9]*$ ]]; then
        echo "Usage: $0 [<integer_greater_than_0>]"
        echo "Error: During demo purpose, you must let system scrap at least 1 article at a time"
        exit 1
    fi
    input=$1
fi
LOG_DIR="~/DE_malay_news/logs/"
LOG_DIR=$(eval echo "$LOG_DIR")

LOG_FILE="~/DE_malay_news/logs/log_$(date +\%Y-\%m-\%d_\%H-\%M-\%S).log"
LOG_FILE=$(eval echo "$LOG_FILE")

WORKING_DIR="~/DE_malay_news"
WORKING_DIR=$(eval echo "$WORKING_DIR")

VENV_DIR="~/de-prj/de-venv/bin/activate"
VENV_DIR=$(eval echo "$VENV_DIR")

cd $WORKING_DIR
source $VENV_DIR
mkdir $LOG_DIR
touch $LOG_FILE
############################## Same as ~/.profile ##############################
################# To set up environment for cron ###############################
# if running bash
if [ -n "$BASH_VERSION" ]; then
    # include .bashrc if it exists
    if [ -f "$HOME/.bashrc" ]; then
        . "$HOME/.bashrc"
    fi
fi

# set PATH so it includes user's private bin if it exists
if [ -d "$HOME/bin" ] ; then
    PATH="$HOME/bin:$PATH"
fi

# set PATH so it includes user's private bin if it exists
if [ -d "$HOME/.local/bin" ] ; then
    PATH="$HOME/.local/bin:$PATH"
fi

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/hduser/hadoop3
export PATH=$PATH:$HADOOP_HOME/bin

export SPARK_HOME=/home/hduser/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=/home/student/de-prj/de-venv/bin/python

export KAFKA_HOME=/home/hduser/kafka
export PATH=$PATH:$KAFKA_HOME/bin

export HBASE_HOME=/home/hduser/hbase 
export PATH=$HBASE_HOME/bin:$PATH

############################################################################################

if [[ -z $input ]]; then
    # Scrap all articles from the specified date time
    echo "Running lexicraft.py..." | tee -a $LOG_FILE 
    ~/de-prj/de-venv/bin/python -u ~/DE_malay_news/run-lexicraft.py 2>&1 | tee -a $LOG_FILE 
else
    echo "Demo run lexicraft.py with scrapping maximum $input article(s)..." | tee -a $LOG_FILE 
    ~/de-prj/de-venv/bin/python -u ~/DE_malay_news/run-lexicraft.py $input 2>&1 | tee -a $LOG_FILE 
fi
deactivate
