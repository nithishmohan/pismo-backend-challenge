# Balance Processor

## Overview

Balance Processor is a PySpark application that processes withdrawals from multiple accounts and updates the account balances accordingly. It ensures that withdrawals are processed in the correct order and updates the balances while recording the results of each transaction.

## Features

- Handles multiple accounts and multiple withdrawals.
- Processes withdrawals in the correct order based on the account and balance order.
- Records the result of each withdrawal, including successful withdrawals, partial withdrawals, and insufficient balance scenarios.
- Uses PySpark for distributed data processing.

## Requirements

- Python 3.6+
- PySpark 3.5.1
- pytest 8.2.1

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/nithishmohan/pismo-backend-challenge.git
    cd <pismo-backend-challenge>
    ```

## Usage

1. Run the setup script to install dependencies:

    ```bash
    scripts/setup.sh
    ```

2. Run the main application:

   ```bash
   scripts/run.sh path_to_balance.csv path_to_withdraw.csv
   ```
   scripts/run.sh balance.csv withdraw.csv will run the balance processor with sample balance & withdraw csvs added inside the processor. Feel free to use your own seed data. 

## Running Tests

To run the unit tests, use the following command:

```bash
scripts/test.sh
 ```

