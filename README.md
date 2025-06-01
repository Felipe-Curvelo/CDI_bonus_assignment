# CDI Bonus Calculation Data Product

## 1. Overview

This project implements a data product to calculate the daily CDI (Certificado de Depósito Interbancário) bonus for user wallet balances. It processes raw transaction data and daily CDI rates to determine user eligibility, calculate interest earned, and generate interest payout transactions.


## 2. How to Run the Service

### 2.1. Dependencies
* An Apache Spark environment with PySpark installed.
* The input files (`Transactions.csv`, `CDIRates.csv`) must be accessible by the Spark application.

### 2.2. Configuration
* Update the input file paths in the `main()` function of the script (`transactions_path`, `cdi_rates_path`).
* Update output paths/table names if not using default Databricks tables (`wallet_history_output_path`, etc.).

### 2.3. Execution
* **Using `spark-submit` (Recommended for standalone execution):**
    ```bash
    spark-submit cdi_interest_calculator.py
    ```
* **In a Databricks Notebook:**
    1.  Upload the script or paste its content into a notebook cell.
    2.  Ensure the input CSV files are accessible (e.g., in DBFS at the specified paths).
    3.  Run the notebook.

## 3. Technical Stack

* **Language:** Python (Version `3.9.21`)
* **Core Framework:** Apache Spark (Version `3.3.2`) via PySpark
* **Development Environment:** The script was developed and tested in a Databricks environment.

## 4. Input Data

The data product consumes two main CSV files - both can be found in the "data" folder::

* **`Transactions.csv`**: Contains raw change data capture (CDC) updates to users' wallets over time.
    * **Columns:**
        * `user_id`: (String) Unique identifier for the user.
        * `timestamp`: (String, format `yyyy-MM-dd HH:mm:ss`) Timestamp of the transaction.
        * `transaction_type`: (String) Type of transaction (e.g., "deposit", "withdrawal").
        * `amount`: (String, convertible to Double) Amount of the transaction.
* **`CDIRates.csv`**: Contains the daily CDI interest rates.
    * **Columns:**
        * `date`: (String, format `yyyy-MM-dd`) The date for which the CDI rate is applicable.
        * `rate`: (String, convertible to Decimal) The daily interest rate.

## 5. Processing Date Range

The overall processing date range for balance history and interest calculation is determined dynamically at runtime. It is derived from the minimum and maximum dates found across both the `Transactions.csv` (based on `transaction_date`) and the `CDIRates.csv` (based on `date`). This ensures all relevant data is considered for generating a complete end-of-day balance history and applying interest calculations.

## 6. Data Structures and Flow (Data Model Considerations)

This data product processes input files and generates intermediate and final data structures using Apache Spark. While the source transactional systems providing the input data might be designed in a normalized form (like 3NF) to ensure data integrity and minimize redundancy, this Spark application focuses on transforming this data for a specific analytical purpose – CDI bonus calculation.

The key DataFrames created and their purpose are:

* **Raw Transactions (`raw_transactions_df`):**
    * Directly loaded from `Transactions.csv` with data type casting (`amount` to Double, `timestamp` to Timestamp, `transaction_date` derived).
* **CDI Rates (`cdi_rates_df`):**
    * Directly loaded from `CDIRates.csv` (`rate` to Decimal, `date` to Date).
* **Wallet History (`wallet_history_intermediate_df`):**
    * Schema: `user_id`, `timestamp`, `transaction_date`, `balance_after_transaction`.
    * Represents each user's balance after every transaction, calculated using a running sum over transactions ordered by time. This is an intermediate table crucial for understanding balance evolution.
* **Daily End-of-Day Balances (`daily_eod_balances_df`):**
    * Schema: `user_id`, `date`, `eod_balance`.
    * Provides the definitive balance for each user at the end of each day (`23:59:59`) within the processing range. It handles days with no transactions by forward-filling the last known balance. Users with no activity prior to a date are assumed to have a zero balance.
* **Eligible Balances for Interest (`eligible_for_interest_df` - conceptual intermediate step):**
    * Determines the `eligible_principal` for interest calculation based on the start-of-day balance and transaction activity on the previous day.
* **Calculated Daily Interest (`daily_interest_df`):**
    * Schema: `user_id`, `interest_date`, `eligible_principal`, `rate`, `interest_earned`.
    * Contains the actual interest amounts calculated for users who met the eligibility criteria on a given day.
* **Interest Payout Transactions (`interest_payout_transactions_df`):**
    * Schema: `user_id`, `timestamp` (set to `interest_date 23:59:59`), `transaction_type` ("interest\_deposit"), `amount` (the `interest_earned`).
    * Formatted as new transactions ready to be potentially ingested back into a transactional system.

This pipeline follows an Extract-Transform-Load (ETL) pattern, where data is read, transformed through various stages to meet business logic, and then prepared for loading/saving. The structures are designed for the specific calculations rather than for general-purpose transactional integrity like in a 3NF OLTP database.

## 7. Design Choices & Logic Explanation

This section details the logic within each major component of the Spark application.

### 7.1. Spark Session Initialization (`create_spark_session`)

* `.config("spark.sql.adaptive.enabled", "true")`: Enables Adaptive Query Execution (AQE) in Spark. This helps improve performance by adjusting execution strategies after shuffle operations. This is set enable by default in more recent Spark versions, the configuration is useful if the application is running on a older version.
* `.config("spark.sql.legacy.timeParserPolicy", "LEGACY")`: This configuration determines how Spark parses and formats date and timestamp strings. Setting it to LEGACY ensures that Spark uses the parsing behavior consistent with older versions of Spark, so the LEGACY policy can be more forgiving of minor variations that might cause errors.

### 7.2. Data Loading and Preparation (`load_transactions`, `load_cdi_rates`)

This crucial first step loads data from `Transactions.csv` and `CDIRates.csv`. Key practices include using `header=True` for column names and `inferSchema=False` to enforce explicit data type definitions.

**`load_transactions(spark, path)`:**
* Reads user transactions and performs essential type conversions:
    * `amount`: Cast to `DoubleType()`. (Note: `DecimalType` would offer higher precision for financial values).
    * `timestamp`: Converted from string to `TimestampType` using the precise format `"yyyy-MM-dd HH:mm:ss"` for accuracy.
    * `transaction_date`: Derived as `DateType` from the `timestamp` for daily operations.
* **Error Handling:** Rows with `timestamp` values that cannot be parsed (resulting in `null`) are filtered out to ensure data quality downstream.

**`load_cdi_rates(spark, path)`:**
* Loads daily CDI rates and converts data types:
    * `rate`: Cast to `DecimalType(10, 8)`, providing high precision suitable for financial rates.
    * `date`: Converted from string to `DateType` using the format `"yyyy-MM-dd"`.
* **Error Handling (Implicit):** Malformed dates resulting in `null` would not join during interest calculation. The quality of the CDI rates file is assumed to be high.

These steps ensure that data entering the core logic is correctly typed and structured, with basic cleaning for critical fields like timestamps.

### 7.3. Wallet History Generation (`create_wallet_history`)

This function constructs a detailed transaction-by-transaction history for each user.
* It first determines the `balance_change` for each transaction (negative for withdrawals, positive otherwise).
* Then, using a Spark window function (`Window.partitionBy("user_id").orderBy("timestamp")`), it calculates the `balance_after_transaction` as a running cumulative sum of `balance_change` for each user, chronologically. This provides a precise balance at every point a transaction occurs.

### 7.4. End-of-Day Balance Calculation (`get_end_of_the_day_balances`)

To ensure accurate daily interest calculation, this function determines each user's wallet balance at the end of every day (`23:59:59`) within the defined processing period.
* A comprehensive `user-date_grid` is created by cross-joining all distinct users with all dates in the processing range. This guarantees a row for every user for every day.
* The balance from the latest transaction on any given day is identified.
* Crucially, balances are then forward-filled: if a user has no transactions on a particular day, the last known balance from a previous day is carried forward. This is achieved using `F.last(balance_after_transaction, ignorenulls=True)` over a window partitioned by user and ordered by date.
* Users with no transaction history prior to a date are assigned an `eod_balance` of `0.0` using `F.coalesce`.
* The function also includes logic to handle invalid date ranges, returning an empty DataFrame with the correct schema if the maximum date is not greater than the minimum date.

### 7.5. Interest Eligibility and Calculation (`calculate_daily_interest`)

This core function applies the business rules to determine interest eligibility and calculate the amounts.
* **Start-of-Day (SOD) Balance:** For each `interest_date`, the SOD balance is derived by taking the End-of-Day (EOD) balance from the previous day (`F.lag("eod_balance", 1, 0.0)`), defaulting to `0.0` for the first day in a user's history.
* **24-Hour Stability Rule:** Users earn interest only if their SOD balance hasn't moved for at least the entire previous calendar day. This is checked by identifying if the user `had_transactions_on_prev_day`. If false, the SOD balance (which was the EOD balance of day D-1 and stable throughout day D-1) is eligible.
* **$100 Threshold Rule:** The `balance_sod` must be greater than $100.
* **Eligible Principal:** Only if both stability and threshold rules are met, the `balance_sod` becomes the `eligible_principal`; otherwise, it's `0.0`.
* **Joining with CDI Rates & Calculation:** The `eligible_principal` is joined with the `cdi_rates_df` on the `interest_date`. The `interest_earned` is calculated as `eligible_principal * daily_rate` and rounded to 4 decimal places. Only records with `interest_earned > 0` are kept.

### 7.6. Formatting Payout Transactions (`format_interest_as_transactions`)

This function transforms the calculated daily interest amounts into a standardized transaction format, ready for potential ingestion.
* Each record of `interest_earned` is converted into a new transaction.
* The `timestamp` for these new transactions is set to `23:59:59` of the `interest_date`, marking the end of the day the interest was earned for.
* A `transaction_type` is assigned as `"interest_deposit"`.
* The `interest_earned` becomes the `amount` of this new transaction.

### 7.7. Output Generation

The final step involves persisting the generated DataFrames.
* Key intermediate and final results, including `wallet_history_intermediate_df`, `daily_eod_balances_df`, `daily_interest_df`, and `interest_payout_transactions_df`, are saved.
* The script uses `write.mode("overwrite")` to ensure that if the job is re-run for the same period, previous outputs are replaced, providing idempotency for the output data itself.
* Operations are written using `saveAsTable()`, common in Databricks environments. The code notes that for other environments, alternatives like `write.parquet("path")` would be used.
* Basic error handling using `try-except` blocks is included for each save operation, printing an error message if a save fails.

## 8. Output Description

The service generates the following outputs (saved as tables):

1.  **`wallet_history`**: Detailed history of transactions and balance after each transaction for every user.
    * Columns: `user_id`, `timestamp`, `transaction_date`, `balance_after_transaction`.
2.  **`daily_eod_balances`**: Each user's balance at the end of every day in the processed range.
    * Columns: `user_id`, `date`, `eod_balance`.
3.  **`daily_interest_calculated`**: Records of daily interest calculated for eligible users.
    * Columns: `user_id`, `interest_date`, `eligible_principal`, `rate`, `interest_earned`.
4.  **`interest_payouts`**: Interest amounts formatted as new deposit transactions.
    * Columns: `user_id`, `timestamp`, `transaction_type`, `amount`.

## 9. Functional Requirements Coverage

The implementation addresses the core functional requirements as follows:

* **Wallet History:** `create_wallet_history` and `get_end_of_the_day_balances` functions generate a comprehensive history of wallet balances.
* **Interest Calculation Rules:**
    * **Balance > $100:** The `calculate_daily_interest` function filters for `balance_sod > 100`.
    * **Balance not moved for 24 hours:** Implemented by checking that there were no transactions (`had_transactions_on_prev_day == False`) on the day preceding the `interest_date`. The interest is calculated on the `balance_sod` (Start of Day balance for `interest_date`, which is the End of Day balance from `interest_date - 1`).
* **Daily Variable Interest Rate:** `calculate_daily_interest` joins with the daily CDI rates.
* **Daily Time Frame (00:00-23:59):** Calculations are based on daily EOD balances, and interest payouts are timestamped at the end of the calculation day.
* **Daily Payout:** `format_interest_as_transactions` prepares daily interest deposits.

## 10. Non-Functional Requirements Addressed

* **Visibility:** The script includes numerous `print` statements to display sample DataFrames at various stages and log messages for key operations (e.g., saving data, empty data checks). This aids in debugging and monitoring the process flow.
* **Error Handling:**
    * Timestamp parsing errors in `load_transactions` result in nulls, which are then filtered out.
    * `get_end_of_the_day_balances` includes a check for invalid date ranges.
    * The `main` function checks if initially loaded DataFrames are empty.
    * Saving operations are wrapped in `try-except` blocks to catch and print potential errors.
* **Modularity & Clarity:** The code is organized into distinct functions with descriptive names, improving readability and maintainability. Variable names are generally clear.

## 11. Compromises or Trade-offs Made

* Due to time constraints, a comprehensive unit and integration testing suite was not developed. In a production scenario, frameworks like `pytest` with Spark testing utilities would be used to validate each function and the overall pipeline logic.
* The current error handling provides logging but does not include automated recovery or alerting mechanisms, which would be added in a production system.
* Configuration (file paths, etc.) is currently hardcoded in the script. For production, this would be externalized to configuration files or environment variables.
* While DecimalType is used for rates, monetary amounts in transactions were loaded as DoubleType for simplicity in this exercise. A production system would enforce DecimalType for all monetary values to prevent precision issues.
* **Interpretation of "Balance Not Moved for 24 Hours" Rule:**
    * **Current Implementation:** The solution interprets the requirement "Users will earn interest on the balance in their wallet that hasn’t been moved for at least 24 hours" in a straightforward manner: *any* transaction (be it a deposit or a withdrawal) recorded on the previous day (Day D-1) means the Start-of-Day balance for the current day (Day D) is considered "moved" and thus ineligible for interest on Day D. This eligibility check is based on the `had_transactions_on_prev_day` flag.
    * **Alternative Consideration:** A business might wish to allow deposits without penalizing interest accrual (as deposits increase the balance), and only consider withdrawals or specific types of debits as "movements" that would make the balance (or a portion of it) ineligible.
    * **Reason for the Chosen Approach (Trade-off):**
        1.  **Simplicity and Clarity of Interpretation:** The current approach adheres to a direct and less ambiguous interpretation of the term "moved" as provided in the problem description.
        2.  **Increased Complexity of a Nuanced Rule:** Implementing a rule where only specific transaction types (like withdrawals) affect eligibility would significantly increase the complexity of the logic:
            * **Defining "Movement":** It would require a clear definition from business stakeholders as to exactly which transaction types reset the stability clock.
            * **Determining Eligible Principal:** If a deposit occurs, does the original stable balance still earn interest, or does the entire new balance need a fresh 24-hour stability period? If a partial withdrawal occurs from a stable balance, does the remaining portion continue to be eligible? This would necessitate more sophisticated logic, possibly tracking different "layers" or "parcels" of money within the balance, or defining a "minimum continuously held balance" throughout the prior day, which is substantially more complex than checking for any transaction.
            * **Data Requirements:** Such logic might also imply a need for more granular data about the *impact* of transactions on specific "parcels" of money, which is beyond the scope of the provided CDC data.
        3.  **Time Constraints and Scope:** Given the context of this assignment, the simpler and more direct interpretation was adopted to deliver a robust and functional solution within a reasonable timeframe. Clarifying and implementing the more complex, nuanced logic would require significant additional specification, development, and testing effort.
    * **Impact:** The current implementation is more conservative in awarding interest, as any transaction on the previous day (including deposits) disqualifies the balance from that day for interest calculation on the subsequent day.
* **Daily Compounding of Interest (Cumulative Interest):**
    * **Current Implementation:** The script calculates daily simple interest based on the eligible Start-of-Day (SOD) balance. The interest earned for a given day is then formatted as a new "interest\_deposit" transaction, which is timestamped for the end of that day. This interest amount is not added back into the balance to earn further interest *within the same execution pass* for subsequent days in the processing period.
    * **Alternative Expectations:** In many systems, interest credited to an account typically becomes part of the principal balance for the next compounding period. Alternatively, some systems might keep earned interest separate, with only the original principal earning further interest, which is functionally similar to the direct outcome of this project's single-run calculation before payout re-ingestion.
    * **Reason for the Chosen Approach (Trade-off):**
        1.  **Simplified Batch Processing Logic:** Implementing true daily compounding *within a single batch execution that processes multiple days* would significantly increase the complexity of the Spark application. The pipeline would need to iteratively:
            * Calculate interest for Day D.
            * Conceptually "deposit" this interest to update the End-of-Day (EOD) balance for Day D.
            * Use this newly augmented EOD balance as the SOD balance for Day D+1.
            This creates a sequential dependency within the daily calculations, making vectorized DataFrame operations across the entire period more challenging and potentially requiring iterative loops or more complex window function chaining to simulate the effect.
        2.  **Clear Separation of Concerns:** The current design clearly separates the calculation of interest based on a defined set of balances from the "payout" of that interest. The output `interest_payouts` DataFrame represents these payouts.
        3.  **Achieving Compounding Across Processing Cycles:** The design implicitly supports compounding over time. It's assumed that the generated "interest\_deposit" transactions would be ingested by the main transactional system. When this CDI bonus calculation job runs for the *next* period, these past interest deposits will naturally be part of the user's balance, and thus, interest will be calculated on them. Compounding therefore occurs across distinct batch processing cycles rather than intra-batch.
    * **Impact:** The interest calculated for a user over a multi-day period in a single job run does not include the effect of interest compounding day-over-day *within that specific run*. For example, interest earned on Monday (and paid out Monday night) does not contribute to the interest-earning balance for Tuesday *in the same batch process*. However, it would contribute if the job is run again for Tuesday after Monday's interest has been posted to the account. This approach simplifies the daily batch calculation considerably.

## 12. Recommendations for Production Ingestion

The generated `interest_payout_transactions_df` (saved as `interest_payouts`) is designed to be ingested into a production transactional database. Recommendations for this process include:

* **Atomicity & Idempotency:** The target ingestion mechanism should ensure that interest payouts are applied atomically and idempotently. If the Spark job writes to a staging location/table, the downstream process loading into the transactional DB could use `MERGE` (UPSERT) operations based on `user_id` and `interest_date` (or a unique transaction ID if generated) to prevent duplicate payouts if the job is re-run.
* **Staging Area:** Write the output to a reliable staging area (e.g., cloud storage as Parquet files, or a staging table).
* **Transactional Integrity:** Use appropriate transaction controls in the database when inserting these new interest transactions.
* **Monitoring & Alerting:** Implement monitoring on the ingestion job to track success/failure and data quality.
* **Scheduling:** Schedule the Spark job and the subsequent ingestion process to run daily after market close or when CDI rates for the day are finalized.
* **Enabling Compounding via Feedback Loop:** To ensure that daily interest can compound as described (i.e., interest earned becomes part of the principal for subsequent calculations), it is essential that the ingestion process feeds the generated `interest_payouts` back into the main transactional system. These ingested deposits must then be accurately reflected in the input transaction data for the *next* execution cycle of this CDI bonus calculation job.

## 13. Potential Future Enhancements

* **Advanced Data Validation:** Implement schema validation for input files (using tools like Great Expectations) and more sophisticated data quality checks (e.g., for anomalous transaction amounts, negative balances).
* **Configuration Management:** Externalize all configurable parameters (file paths, output table names, thresholds) using a configuration file (e.g., YAML, JSON) or environment variables.
* **Robust Logging & Alerting:** Integrate with a centralized logging system and set up alerts for failures or critical issues.
* **Comprehensive Testing:** Develop unit tests for individual functions (especially complex transformations) and integration tests for the end-to-end pipeline.
* **Delta Lake Integration:** For enhanced reliability, ACID transactions, and time travel capabilities on the output tables, consider using Delta Lake format instead of basic Parquet or Hive tables, especially if operating in an environment like Databricks.
* **Parameterization of Rules:** Some rules where configured in the script (like the $100 threshold) consider making all of them configurable.
* **Workflow Orchestration:** Integrate the application into a data pipeline orchestrator (e.g., Airflow, ADF) for improved end-to-end manageability of the entire CDI bonus calculation workflow.
