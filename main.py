from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from balance_processor import BalanceProcessor


class BalanceProcessorApp:
    def __init__(self, balance_data: List[Tuple[int, int, float, str]], withdraw_data: List[Tuple[int, int, float]]):
        """
        Initialize the BalanceProcessorApp with balance and withdrawal data.

        :param balance_data: List of tuples containing balance data
        :param withdraw_data: List of tuples containing withdrawal data
        """
        self.withdraw_df: DataFrame = None
        self.balance_df: DataFrame = None
        self.result_df: DataFrame = None
        self.balance_data = balance_data
        self.withdraw_data = withdraw_data
        self.spark = SparkSession.builder.appName("BalanceWithdraw").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.processor = BalanceProcessor(self.spark)

    def create_dataframes(self) -> None:
        """
        Create balance and withdrawal DataFrames using the BalanceProcessor.
        """
        self.balance_df = self.processor.create_balance_df(self.balance_data)
        self.withdraw_df = self.processor.create_withdraw_df(self.withdraw_data)

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
        self.create_dataframes()
        self.process_withdrawals()
        self.show_results()
        self.stop_spark()


if __name__ == "__main__":
    # Sample data for Balance and Withdraw tables
    balance_data = [(1, 2, 100.0, "AVAILABLE"), (1, 1, 50.0, "AVAILABLE")]
    withdraw_data = [(1, 2, 30.0), (1, 1, 80.0), (1, 3, 100.0), (1, 4, 20.0)]

    # Initialize and run the BalanceProcessorApp
    app = BalanceProcessorApp(balance_data, withdraw_data)
    app.run()
