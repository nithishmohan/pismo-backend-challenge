import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, DoubleType, StructField, IntegerType, StructType

from balance_processor import BalanceProcessor


class BalanceProcessorTest(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("BalanceProcessorTest") \
            .master("local[*]") \
            .getOrCreate()
        cls.processor = BalanceProcessor(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.balance_data = [
            (1, 1, 100.0, "AVAILABLE"),
            (1, 2, 50.0, "AVAILABLE")
        ]
        self.withdraw_data = [
            (1, 2, 30.0),
            (1, 1, 80.0)
        ]

    def test_process_withdrawals(self):
        balance_df = self.processor.create_balance_df(self.balance_data)
        withdraw_df = self.processor.create_withdraw_df(self.withdraw_data)

        result_df = self.processor.process_withdrawals(balance_df, withdraw_df)

        expected_data = [
            (1, 1, 100.0, 20.0, "AVAILABLE", "Withdraw 80.0 successful, 80.0 withdrawn"),
            (1, 1, 20.0, 0.0, "BALANCE WITHDREW", "Withdraw 30.0 partially successful, 20.0 withdrawn, 10.0 remaining"),
            (1, 2, 50.0, 40.0, "AVAILABLE", "Withdraw 30.0 successful, 10.0 withdrawn")
        ]

        expected_df = self.spark.createDataFrame(expected_data, schema=result_df.schema)

        self.assertEqual(result_df.count(), expected_df.count())
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_insufficient_balance(self):
        balance_data = [
            (1, 1, 10.0, "AVAILABLE"),
            (1, 2, 20.0, "AVAILABLE")
        ]
        withdraw_data = [
            (1, 1, 50.0)
        ]

        balance_df = self.processor.create_balance_df(balance_data)
        withdraw_df = self.processor.create_withdraw_df(withdraw_data)

        result_df = self.processor.process_withdrawals(balance_df, withdraw_df)

        expected_data = [
            (1, None, None, None, None, "Withdraw 50.0 failed, insufficient balance"),
            (1, 1, 10.0, 10.0, "AVAILABLE", ""),
            (1, 2, 20.0, 20.0, "AVAILABLE", "")
        ]

        expected_schema = StructType([
            StructField("account_id", IntegerType(), True),
            StructField("balance_order", IntegerType(), True),
            StructField("initial_balance", DoubleType(), True),
            StructField("available_balance", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("validation_result", StringType(), True)
        ])

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(result_df.count(), expected_df.count())
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_multiple_accounts(self):
        balance_data = [
            (1, 1, 100.0, "AVAILABLE"),
            (1, 2, 50.0, "AVAILABLE"),
            (2, 1, 200.0, "AVAILABLE"),
            (2, 2, 150.0, "AVAILABLE")
        ]
        withdraw_data = [
            (1, 1, 30.0),
            (2, 1, 100.0)
        ]

        balance_df = self.processor.create_balance_df(balance_data)
        withdraw_df = self.processor.create_withdraw_df(withdraw_data)

        result_df = self.processor.process_withdrawals(balance_df, withdraw_df)

        expected_data = [
            (1, 1, 100.0, 70.0, "AVAILABLE", "Withdraw 30.0 successful, 30.0 withdrawn"),
            (2, 1, 200.0, 100.0, "AVAILABLE", "Withdraw 100.0 successful, 100.0 withdrawn"),
            (1, 2, 50.0, 50.0, "AVAILABLE", ""),
            (2, 2, 150.0, 150.0, "AVAILABLE", "")
        ]

        expected_df = self.spark.createDataFrame(expected_data, schema=result_df.schema)

        self.assertEqual(result_df.count(), expected_df.count())
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_no_withdrawals(self):
        balance_df = self.processor.create_balance_df(self.balance_data)
        withdraw_df = self.processor.create_withdraw_df([])

        result_df = self.processor.process_withdrawals(balance_df, withdraw_df)

        expected_data = [
            (1, 1, 100.0, 100.0, "AVAILABLE", ""),
            (1, 2, 50.0, 50.0, "AVAILABLE", "")
        ]

        expected_df = self.spark.createDataFrame(expected_data, schema=result_df.schema)

        self.assertEqual(result_df.count(), expected_df.count())
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_no_balances(self):
        balance_df = self.processor.create_balance_df([])
        withdraw_df = self.processor.create_withdraw_df(self.withdraw_data)

        result_df = self.processor.process_withdrawals(balance_df, withdraw_df)

        expected_data = [
            (1, None, None, None, None, "Withdraw 80.0 failed, insufficient balance"),
            (1, None, None, None, None, "Withdraw 30.0 failed, insufficient balance")
        ]

        expected_schema = StructType([
            StructField("account_id", IntegerType(), True),
            StructField("balance_order", IntegerType(), True),
            StructField("initial_balance", DoubleType(), True),
            StructField("available_balance", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("validation_result", StringType(), True)
        ])

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(result_df.count(), expected_df.count())
        self.assertEqual(result_df.collect(), expected_df.collect())


