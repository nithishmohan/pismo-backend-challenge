from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField


class BalanceProcessor:
    def __init__(self, spark: SparkSession):
        """
        Initialize the BalanceProcessor with a Spark session.

        :param spark: SparkSession object
        """
        self.spark = spark

    def create_balance_df(self, balance_data: List[Tuple[int, int, float, str]]) -> DataFrame:
        """
        Create a DataFrame for balances with the given data.
        Adds an initial_balance column that is the same as available_balance initially.

        :param balance_data: List of tuples containing balance data (account_id, balance_order, available_balance, status)
        :return: DataFrame containing balance data
        """
        balance_schema = StructType([
            StructField("account_id", IntegerType(), False),
            StructField("balance_order", IntegerType(), False),
            StructField("available_balance", DoubleType(), False),
            StructField("status", StringType(), False)
        ])
        balance_df = self.spark.createDataFrame(balance_data, schema=balance_schema)
        balance_df = balance_df.withColumn("initial_balance", col("available_balance"))
        return balance_df

    def create_withdraw_df(self, withdraw_data: List[Tuple[int, int, float]]) -> DataFrame:
        """
        Create a DataFrame for withdrawals with the given data.

        :param withdraw_data: List of tuples containing withdrawal data (account_id, withdraw_order, withdraw_amount)
        :return: DataFrame containing withdrawal data
        """
        withdraw_schema = StructType([
            StructField("account_id", IntegerType(), False),
            StructField("withdraw_order", IntegerType(), False),
            StructField("withdraw_amount", DoubleType(), False)
        ])
        return self.spark.createDataFrame(withdraw_data, schema=withdraw_schema)

    def process_withdrawals(self, balance_df: DataFrame, withdraw_df: DataFrame) -> DataFrame:
        """
        Process the withdrawals from the balances.
        Returns a DataFrame with the results of the withdrawals.

        :param balance_df: DataFrame containing balance data
        :param withdraw_df: DataFrame containing withdrawal data
        :return: DataFrame containing the result of withdrawal processing
        """
        balance_df = balance_df.orderBy(["account_id", "balance_order"])
        withdraw_df = withdraw_df.orderBy(["account_id", "withdraw_order"])
        withdrawals = withdraw_df.collect()
        result: List[Row] = []

        for withdraw in withdrawals:
            account_id = withdraw["account_id"]
            withdraw_amount = withdraw["withdraw_amount"]
            account_balances = balance_df.filter(
                (col("account_id") == account_id) & (col("status") != "BALANCE WITHDREW")
            ).orderBy("balance_order").collect()
            remaining_withdraw = withdraw_amount

            transaction_changes: List[Tuple[Row, float, float, str, str]] = []
            successful = self.process_single_withdrawal(
                account_id, withdraw_amount, account_balances, remaining_withdraw, transaction_changes, result
            )

            if successful:
                balance_df = self.apply_transaction_changes(balance_df, transaction_changes)
                result.extend(self.create_transaction_results(transaction_changes))

        result = self.add_unaffected_balances(balance_df, result)
        return self.create_result_df(result)

    @staticmethod
    def process_single_withdrawal(
            account_id: int, withdraw_amount: float, account_balances: List[Row],
            remaining_withdraw: float, transaction_changes: List[Tuple[Row, float, float, str, str]],
            result: List[Row]
    ) -> bool:
        """
        Process a single withdrawal request.
        Updates the balances and records the changes.

        :param account_id: Account ID for the withdrawal
        :param withdraw_amount: Amount to withdraw
        :param account_balances: List of Row objects representing account balances
        :param remaining_withdraw: Remaining amount to withdraw
        :param transaction_changes: List to record transaction changes
        :param result: List to record the result of the withdrawal
        :return: Boolean indicating if the withdrawal was successful
        """
        for balance in account_balances:
            available_balance = balance["available_balance"]

            if available_balance >= remaining_withdraw:
                new_available_balance = float(available_balance - remaining_withdraw)
                withdrawn_amount = remaining_withdraw
                remaining_withdraw = 0.0
                validation_result = f"Withdraw {withdraw_amount} successful, {withdrawn_amount} withdrawn"
                transaction_changes.append((
                    balance, new_available_balance, balance["initial_balance"],
                    "AVAILABLE" if new_available_balance > 0 else "BALANCE WITHDREW", validation_result
                ))
                return True
            else:
                new_available_balance = 0.0
                withdrawn_amount = available_balance
                remaining_withdraw -= available_balance
                validation_result = f"Withdraw {withdraw_amount} partially successful, {withdrawn_amount} withdrawn, {remaining_withdraw} remaining"
                transaction_changes.append((
                    balance, new_available_balance, balance["initial_balance"], "BALANCE WITHDREW", validation_result
                ))

        if remaining_withdraw > 0:
            result.append(Row(
                account_id=account_id,
                balance_order=None,
                initial_balance=None,
                available_balance=None,
                status=None,
                validation_result=f"Withdraw {withdraw_amount} failed, insufficient balance"
            ))
            return False
        return True

    def apply_transaction_changes(self, balance_df: DataFrame,
                                  transaction_changes: List[Tuple[Row, float, float, str, str]]) -> DataFrame:
        """
        Apply the changes to the balance DataFrame.

        :param balance_df: DataFrame containing balance data
        :param transaction_changes: List of transaction changes to apply
        :return: Updated DataFrame with the transaction changes applied
        """
        for change in transaction_changes:
            balance, new_available_balance, initial_balance, new_status, validation_result = change
            balance_df = self.update_balance(balance_df, balance, new_available_balance, new_status)
        return balance_df

    def create_transaction_results(self, transaction_changes: List[Tuple[Row, float, float, str, str]]) -> List[Row]:
        """
        Create result rows for each transaction change.

        :param transaction_changes: List of transaction changes
        :return: List of Row objects representing the results of the transactions
        """
        return [self.create_result_row(balance, new_available_balance, validation_result) for
                balance, new_available_balance, initial_balance, new_status, validation_result in transaction_changes]

    def add_unaffected_balances(self, balance_df: DataFrame, result: List[Row]) -> List[Row]:
        """
        Add balances that were not affected by any withdrawals to the result.

        :param balance_df: DataFrame containing balance data
        :param result: List of Row objects representing the results of the transactions
        :return: Updated list of Row objects with unaffected balances added
        """
        unaffected_balances = balance_df.filter(col("available_balance") == col("initial_balance")).collect()
        for balance in unaffected_balances:
            result.append(self.create_result_row(balance, balance["available_balance"], ""))
        return result

    @staticmethod
    def create_result_row(balance: Row, new_available_balance: float, validation_result: str) -> Row:
        """
        Create a result row for a transaction change.

        :param balance: Row object representing the balance
        :param new_available_balance: New available balance after the transaction
        :param validation_result: Result of the transaction validation
        :return: Row object representing the result of the transaction
        """
        return Row(
            account_id=balance["account_id"],
            balance_order=balance["balance_order"],
            initial_balance=balance['available_balance'],
            available_balance=new_available_balance,
            status="AVAILABLE" if new_available_balance > 0 else "BALANCE WITHDREW",
            validation_result=validation_result
        )

    @staticmethod
    def update_balance(balance_df: DataFrame, balance: Row, new_available_balance: float, new_status: str) -> DataFrame:
        """
        Update the balance DataFrame with new available balance and status.

        :param balance_df: DataFrame containing balance data
        :param balance: Row object representing the balance to update
        :param new_available_balance: New available balance after the transaction
        :param new_status: New status after the transaction
        :return: Updated DataFrame with the balance changes applied
        """
        return balance_df.withColumn("available_balance", when(
            (col("account_id") == balance["account_id"]) & (col("balance_order") == balance["balance_order"]),
            new_available_balance
        ).otherwise(col("available_balance"))).withColumn("status", when(
            (col("account_id") == balance["account_id"]) & (col("balance_order") == balance["balance_order"]),
            new_status
        ).otherwise(col("status")))

    def create_result_df(self, result: List[Row]) -> DataFrame:
        """
        Create the final result DataFrame from the result rows.

        :param result: List of Row objects representing the results of the transactions
        :return: DataFrame containing the results of the transactions
        """
        result_schema = StructType([
            StructField("account_id", IntegerType(), True),
            StructField("balance_order", IntegerType(), True),
            StructField("initial_balance", DoubleType(), True),
            StructField("available_balance", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("validation_result", StringType(), True)
        ])
        result_df = self.spark.createDataFrame(result, schema=result_schema)
        return result_df
