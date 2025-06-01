# Import libraries for later use
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StructType, StructField, DateType, DecimalType
import datetime


def create_spark_session():
    # Creates and returns a SparkSession
    spark = SparkSession.builder \
        .appName("CDIBonusCalculation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def load_transactions(spark, path):
    # Loads transaction data from CSV
    df = spark.read.csv(path, header=True, inferSchema=False)
    df = df.withColumn("amount", F.col("amount").cast(DoubleType())) \
           .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("transaction_date", F.to_date(F.col("timestamp")))
    # Filter out any rows where timestamp parsing might have failed (resulting in null)
    df = df.filter(F.col("timestamp").isNotNull())
    return df


def load_cdi_rates(spark, path):
    # Loads CDI rates from CSV.
    df = spark.read.csv(path, header=True, inferSchema=False)
    df = df.withColumn("rate", F.col("rate").cast(DecimalType(10, 8))) \
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    return df


def create_wallet_history(transactions_df):
    # Creates an intermediate table history to identify wallet balances over time
    window_spec_runing_balance = Window.partitionBy("user_id") \
        .orderBy("timestamp") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    wallet_history_df = transactions_df \
        .withColumn("balance_change", F.when(F.col("transaction_type") == "withdrawal", -F.col("amount")).otherwise(F.col("amount"))) \
        .withColumn("balance_after_transaction", F.sum("balance_change").over(window_spec_runing_balance))

    print("\n--- Wallet History (Sample) ---")
    wallet_history_df.select("user_id", "timestamp", "transaction_type", "amount", "balance_after_transaction") \
        .orderBy("user_id", "timestamp").show(30, truncate=False)
    return wallet_history_df.select("user_id", "timestamp", "transaction_date", "balance_after_transaction")


# Calculates the end-of-day balance for each user for each day in the period
def get_end_of_the_day_balances(wallet_history_df, min_date: datetime.date, max_date: datetime.date):
    # retrieve the spark session from existing Dataframe to create the all_dates_df dataframe
    spark = wallet_history_df.sparkSession

    num_days = (max_date - min_date).days + 1
    if num_days <= 0:  # edge case if max_date is not greater than min_date
        schema = StructType([
            StructField("user_id", type(
                wallet_history_df.schema["user_id"].dataType)(), True),
            StructField("date", DateType(), True),
            StructField("eod_balance", DoubleType(), True)
        ])
        print(
            f"Warning: Invalid date range for daily balances (minimun date:{min_date} to maximum date:{max_date}). Returning empty balances.")
        return spark.createDataFrame([], schema)

    dates_schema = StructType([
        StructField("date", DateType(), True)
    ])

    # Create a Dataframe with all dates no matter if the transactions has gaps between on date and another
    all_dates_df = spark.createDataFrame(
        [Row(date=min_date + datetime.timedelta(days=i)) for i in range(num_days)], dates_schema)

    distinct_users_df = wallet_history_df.select("user_id").distinct()
    user_date_grid_df = distinct_users_df.crossJoin(all_dates_df)

    window_last_txn_of_day = Window.partitionBy(
        "user_id", "transaction_date").orderBy(F.col("timestamp").desc())

    balance_from_latest_txn_df = wallet_history_df \
        .withColumn("rn", F.expr("row_number()").over(window_last_txn_of_day)) \
        .filter(F.col("rn") == 1) \
        .select(
            F.col("user_id"),
            F.col("transaction_date").alias("date"),
            F.col("balance_after_transaction")
        )

    udg_alias = user_date_grid_df.alias("udg")
    bflt_alias = balance_from_latest_txn_df.alias("bflt")

    joined_for_fill_df = udg_alias.join(
        bflt_alias,
        (udg_alias.user_id == bflt_alias.user_id) & (
            udg_alias.date == bflt_alias.date),
        "left"
    )

    window_fill_forward = Window.partitionBy(udg_alias.user_id) \
        .orderBy(udg_alias.date) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    daily_eod_balances = joined_for_fill_df \
        .withColumn("eod_balance_filled", F.last(bflt_alias.balance_after_transaction, ignorenulls=True).over(window_fill_forward)) \
        .select(
            udg_alias.user_id,
            udg_alias.date,
            F.coalesce(F.col("eod_balance_filled"),
                       F.lit(0.0)).alias("eod_balance")
        ).distinct()

    print("\n--- Daily End of Day Balances (Sample) ---")

    if daily_eod_balances.head(1):
        daily_eod_balances.orderBy("user_id", "date").show(30, truncate=False)
    else:
        print("Daily End of Day Balances DataFrame is empty.")
    return daily_eod_balances


"""
    Calculates daily interest based on eligibility rules:
    - Balance > $100 at the start of the day
    - Balance was not moved (no transactions) during the entire previous day
"""


def calculate_daily_interest(daily_eod_balances_df, transactions_df, cdi_rates_df):
    # 1. Determine balance at the start of day D (which is EOD balance of D-1)
    window_spec_lag_balance = Window.partitionBy("user_id").orderBy("date")
    balance_sod_df = daily_eod_balances_df \
        .withColumn("balance_sod", F.lag("eod_balance", 1, 0.0).over(window_spec_lag_balance)) \
        .select("user_id", F.col("date").alias("interest_date"), "balance_sod")

    # 2. Determine if there were any transactions on day D-1
    transactions_activity_df = transactions_df \
        .select("user_id", "transaction_date") \
        .distinct() \
        .withColumn("had_transactions_on_activity_date", F.lit(True)) \
        .withColumnRenamed("transaction_date", "activity_date")

    # 3. Combine for elegibility check
    bsod_alias = balance_sod_df.alias("bsod")
    ta_alias = transactions_activity_df.alias("ta")

    eligibility_df = bsod_alias.join(
        ta_alias,
        (bsod_alias.user_id == ta_alias.user_id) &
        (F.date_sub(bsod_alias.interest_date, 1) ==
         ta_alias.activity_date),  # activity_date is D-1
        "left"
    ) \
        .select(
        bsod_alias.user_id,
        bsod_alias.interest_date,
        bsod_alias.balance_sod,
        F.coalesce(ta_alias.had_transactions_on_activity_date,
                   F.lit(False)).alias("had_transactions_on_prev_day")
    )

    # 4. Apply eligibility rules
    eligible_for_interest_df = eligibility_df \
        .withColumn("is_elegible",
                    (F.col("balance_sod") > 100) & (
                        F.col("had_transactions_on_prev_day") == False)
                    ) \
        .withColumn("eligible_principal", F.when(F.col("is_elegible"), F.col("balance_sod")).otherwise(0.0)) \
        .select("user_id", "interest_date", "eligible_principal", "balance_sod", "had_transactions_on_prev_day")

    print("\n--- Elegibility Check (Sample) ---")
    eligible_for_interest_df.orderBy("user_id", "interest_date").filter(
        F.col("eligible_principal") > 0).show(30, truncate=False)

    # 5. Join with CDI rates for the interest date
    efi_alias = eligible_for_interest_df.alias("efi")
    cdi_alias = cdi_rates_df.alias("cdi")

    interest_calculation_df = efi_alias \
        .join(cdi_alias, efi_alias.interest_date == cdi_alias.date) \
        .withColumn("interest_earned", F.round(efi_alias.eligible_principal * cdi_alias.rate, 4)) \
        .filter(F.col("interest_earned") > 0) \
        .select(
            efi_alias.user_id,
            efi_alias.interest_date,
            efi_alias.eligible_principal,
            cdi_alias.rate,
            F.col("interest_earned")
        )

    print("\n--- Calculated Daily Interest (Sample) ---")
    interest_calculation_df.orderBy(
        "user_id", "interest_date").show(truncate=False)
    return interest_calculation_df


def format_interest_as_transactions(daily_interest_df):
    # Formats calculated interest as deposit transactions
    interest_transactions_df = daily_interest_df \
        .withColumn("timestamp", F.expr("to_timestamp(concat(interest_date, ' 23:59:59'), 'yyyy-MM-dd HH:mm:ss')")) \
        .withColumn("transaction_type", F.lit("interest_deposit")) \
        .select(
            F.col("user_id"),
            F.col("timestamp"),
            F.col("transaction_type"),
            F.col("interest_earned").alias("amount")
        )
    print("\n--- Interest Payout Transactions (Sample) ---")
    interest_transactions_df.orderBy(
        "user_id", "timestamp").show(truncate=False)
    return interest_transactions_df


def main():
    spark = create_spark_session()
    # Define input paths
    # Assuming Databricks path, adjust if needed
    transactions_path = "/FileStore/tables/Transactions.csv"
    # Assuming Databricks path, adjust if needed
    cdi_rates_path = "/FileStore/tables/CDIRates.csv"

    # Define output paths
    wallet_history_output_path = "wallet_history"
    interest_payouts_output_path = "interest_payouts"
    daily_eod_balances_output_path = "daily_eod_balances"
    daily_interest_calculated_output_path = "daily_interest_calculated"

    raw_transactions_df = load_transactions(spark, transactions_path)
    print("\n--- Raw Transactions (Sample) ---")
    if raw_transactions_df.head(1):
        raw_transactions_df.show(5, truncate=False)
    else:
        print("Raw Transactions DataFrame is empty or loading failed.")

        return

    cdi_rates_df = load_cdi_rates(spark, cdi_rates_path)
    print("\n--- CDI Rates (Sample) ---")
    if cdi_rates_df.head(1):
        cdi_rates_df.show(5, truncate=False)
    else:
        print("CDI Rates DataFrame is empty or loading failed.")

        return

    wallet_history_intermediate_df = create_wallet_history(raw_transactions_df)

    min_max_tx_dates_row = raw_transactions_df.agg(
        F.expr("min(transaction_date) as min_tx_date"),
        F.expr("max(transaction_date) as max_tx_date")
    ).first()

    min_max_cdi_dates_row = cdi_rates_df.agg(
        F.expr("min(date) as min_cdi_date"),
        F.expr("max(date) as max_cdi_date")
    ).first()

    all_min_dates = [d for d in [
        min_max_tx_dates_row.min_tx_date, min_max_cdi_dates_row.min_cdi_date] if d]
    all_max_dates = [d for d in [
        min_max_tx_dates_row.max_tx_date, min_max_cdi_dates_row.max_cdi_date] if d]

    if not all_min_dates or not all_max_dates:
        print(
            "Error: Could not determine a valid date range from transactions or CDI rates.")
        spark.stop()
        return

    min_overall_date = min(all_min_dates)
    max_overall_date = max(all_max_dates)
    print(
        f"Overall date range for processing: {min_overall_date} to {max_overall_date}")

    daily_eod_balances_df = get_end_of_the_day_balances(
        wallet_history_intermediate_df, min_overall_date, max_overall_date)

    if not daily_eod_balances_df.head(1):
        print("Daily End of Day Balances are empty. Interest calculation will likely yield no results.")
    else:
        print("Daily End of Day Balances calculated successfully.")

    daily_interest_df = calculate_daily_interest(
        daily_eod_balances_df, raw_transactions_df, cdi_rates_df)

    interest_payout_transactions_df = format_interest_as_transactions(
        daily_interest_df)

    if interest_payout_transactions_df.head(1):
        print("Interest was successfully calculated.")
    else:
        print("No results for interest calculation.")

    print("\n -*-*-*-* Starting to save the results -*-*-*-*")

    print(f"\n--- Saving wallet history: {wallet_history_output_path} ---")

    # Change the format of saving in case of this code is running outside Databricks environment
    try:
        wallet_history_intermediate_df.write.mode(
            "overwrite").saveAsTable(wallet_history_output_path)
        print(f"Sucessfuly saved Wallet History")
    except Exception as e:
        print(f"Error saving Wallet History: {e}")

    print(
        f"\n--- Saving Interest Payout Transactions: {interest_payouts_output_path} ---")

    try:
        interest_payout_transactions_df.write.mode(
            "overwrite").saveAsTable(interest_payouts_output_path)
        print(f"Successfully saved Interest Payout Transactions")
    except Exception as e:
        print(f"Error saving Interest Payout Transactions: {e}")

    print(
        f"\n--- Saving Daily End of the Day Balances: {daily_eod_balances_output_path} ---")

    try:
        daily_eod_balances_df.write.mode("overwrite").saveAsTable(
            daily_eod_balances_output_path)
        print(f"Successfully saved Daily End of the Day Balances")
    except Exception as e:
        print(f"Error saving Daily End of the Day Balances: {e}")

    print(
        f"\n--- Saving Daily Interest Calculated: {daily_interest_calculated_output_path} ---")

    try:
        daily_interest_df.write.mode("overwrite").saveAsTable(
            daily_interest_calculated_output_path)
        print(f"Successfully saved Daily Interest Calculated")
    except Exception as e:
        print(f"Error saving Daily Interest Calculated: {e}")

    print("\n--- CDI Bonus Calculation Process Completed ---")


if __name__ == "__main__":
    main()
