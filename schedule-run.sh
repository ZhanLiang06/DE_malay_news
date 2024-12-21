#!/bin/bash

# Only one or two arguments will be accepted
if [ "$#" -eq 0 ] || [ "$#" -gt 2 ]; then
    echo "Only one or two arguments will be accepted"
    echo "Usage:"
    echo "  First option: $0 <demo_num_of_article> <cron_schedule>"
    echo "  Second option: $0 <cron_schedule>"
    echo "Example:"
    echo "  First option: $0 2 '0 * * * *'"
    echo "  Second option: $0 '0 * * * *'"
    exit 1
fi

SCRIPT_TO_REGISTER="~/DE_malay_news/run-lexicraft.sh"
SCRIPT_TO_REGISTER=$(eval echo "$SCRIPT_TO_REGISTER")

# Validate on the input
if [ "$#" -eq 2 ]; then
    PYTHON_SCRIPT_ARG="$1"
    CRON_SCHEDULE="$2"

    # Validate the first argument (must be an integer greater than 1)
    if ! [[ "$PYTHON_SCRIPT_ARG" =~ ^[0-9]+$ ]] || [ "$PYTHON_SCRIPT_ARG" -le 0 ]; then
        echo "Error: The first argument must be an integer greater than 1."
        echo "Error: During demo purpose, you must let system scrap at least 1 article at a time"
        exit 1
    fi

    # Validate the second argument (must be a valid cron schedule)
    if ! [[ "$CRON_SCHEDULE" =~ ^([0-9*,-]+[[:space:]]+){4}[0-9*,-]+$ ]]; then
        echo "Error: The second argument must be a valid cron schedule."
        exit 1
    fi

    # Command to add to crontab
    CRON_COMMAND="$CRON_SCHEDULE $SCRIPT_TO_REGISTER $PYTHON_SCRIPT_ARG"

elif [ "$#" -eq 1 ]; then
    # Second option: register.sh <cron_schedule>
    CRON_SCHEDULE="$1"

    # Validate the schedule (must be a valid cron schedule)
    if ! [[ "$CRON_SCHEDULE" =~ ^([0-9*,-]+[[:space:]]+){4}[0-9*,-]+$ ]]; then
        echo "Error: The argument must be a valid cron schedule."
        exit 1
    fi

    # Command to add to crontab
    CRON_COMMAND="$CRON_SCHEDULE $SCRIPT_TO_REGISTER"
else
    # may be redundant
    echo "Usage:"
    echo "  First option: $0 <demo_num_of_article> <cron_schedule>"
    echo "  Second option: $0 <cron_schedule>"
    exit 1
fi

# Ensure the script exists
if [ ! -f "$SCRIPT_TO_REGISTER" ]; then
    echo "Error: Script $SCRIPT_TO_REGISTER does not exist."
    exit 1
fi

# Backup existing crontab
crontab -l > crontab_backup.txt 

# Check if the script is already registered in crontab
if crontab -l | grep -q "$SCRIPT_TO_REGISTER"; then
    echo "Script $SCRIPT_TO_REGISTER is already registered in crontab."
else
    # Add the script to crontab
    (crontab -l ; echo "$CRON_COMMAND") | crontab -
    #echo "Script $SCRIPT_TO_REGISTER has been added to crontab with command: $CRON_COMMAND"
fi
