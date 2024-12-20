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

cd ~/DE_malay_news
source ~/de-prj/de-venv/bin/activate
if [[ -z $input ]]; then
    # Scrap all articles from the specified date time
    echo "Running lexicraft.py..."
    ~/de-prj/de-venv/bin/python ~/DE_malay_news/run-lexicraft.py
else
    echo "Demo run lexicraft.py with scrapping maximum $input article(s)..."
    ~/de-prj/de-venv/bin/python ~/DE_malay_news/run-lexicraft.py $input
fi
deactivate
