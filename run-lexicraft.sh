#!/bin/bash
source ~/de-prj/de-venv/bin/activate
echo "Running lexicraft.py"
~/de-prj/de-venv/bin/python ~/DE_malay_news/run-lexicraft.py $1 $2 $3 $4
deactivate
