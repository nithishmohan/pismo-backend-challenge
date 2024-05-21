#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <path_to_balance_csv> <path_to_withdraw_csv>"
    exit 1
fi

# Assign arguments to variables
BALANCE_CSV=$1
WITHDRAW_CSV=$2

# Activate the virtual environment
source venv/bin/activate

echo "Running Balance Processor.."
# Run the main application with CSV file paths as arguments
python main.py "$BALANCE_CSV" "$WITHDRAW_CSV"
