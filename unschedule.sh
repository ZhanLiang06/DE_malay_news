#!/bin/bash

SCRIPT_TO_UNREGISTER="run-lexicraft.sh"

#Back up
crontab -l > crontab_backup.txt 2>/dev/null

if crontab -l | grep -q "$SCRIPT_TO_UNREGISTER"; then
    # Remove the line containing the script from the crontab
    (crontab -l 2>/dev/null | grep -v "$SCRIPT_TO_UNREGISTER") | crontab -
    echo "Script $SCRIPT_TO_UNREGISTER has been unregistered from crontab."
else
    echo "Script $SCRIPT_TO_UNREGISTER is not registered in crontab."
fi