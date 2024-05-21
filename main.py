from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from balance_processor import BalanceProcessor


class BalanceProcessorApp:
    def __init__(self, balance_csv: str, withdraw_csv: str):
        """
        Initialize the BalanceProcessorApp with paths to balance and withdrawal CSV files.

        :param balance_csv: Path to the balance CSV file
        :param withdraw_csv: Path to the withdraw CSV file
        """
        self.result_df = None
        self.balance_df = None
        self.withdraw_df = None
        self.balance_csv = balance_csv
        self.withdraw_csv = withdraw_csv
        self.spark = SparkSession.builder.appName("BalanceWithdraw").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.processor = BalanceProcessor(self.spark)

    def create_dataframes(self) -> None:
        """
            Create balance and withdrawal DataFrames by reading from CSV files and setting correct data types.
        """
        # Read the CSV files
        raw_balance_df = self.spark.read.csv(self.balance_csv, header=True, inferSchema=True)
        raw_withdraw_df = self.spark.read.csv(self.withdraw_csv, header=True, inferSchema=True)

        # Convert the DataFrames using the processor's methods to set correct data types
        balance_data = [
            (int(row.account_id), int(row.balance_order), float(row.available_balance), str(row.status))
            for row in raw_balance_df.collect()
        ]
        withdraw_data = [
            (int(row.account_id), int(row.withdraw_order), float(row.withdraw_amount))
            for row in raw_withdraw_df.collect()
        ]

        self.balance_df = self.processor.create_balance_df(balance_data)
        self.withdraw_df = self.processor.create_withdraw_df(withdraw_data)

    def process_withdrawals(self) -> None:
        """
        Process the withdrawals and store the result DataFrame.
        """
        self.result_df = self.processor.process_withdrawals(self.balance_df, self.withdraw_df)

    def show_results(self) -> None:
        """
        Display the results of the withdrawals.
        """
        self.result_df.show(truncate=False)

    def stop_spark(self) -> None:
        """
        Stop the Spark session.
        """
        self.spark.stop()

    def run(self) -> None:
        """
        Run the BalanceProcessorApp to process withdrawals and display results.
        """
        print("\n\n\n\nStarting BalanceProcessorApp, Please wait for processing...")
        self.create_dataframes()
        self.process_withdrawals()
        self.show_results()
        self.stop_spark()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python main.py <path_to_balance_csv> <path_to_withdraw_csv>")
        sys.exit(1)

    balance_csv = sys.argv[1]
    withdraw_csv = sys.argv[2]

    # Initialize and run the BalanceProcessorApp
    app = BalanceProcessorApp(balance_csv, withdraw_csv)
    app.run()
