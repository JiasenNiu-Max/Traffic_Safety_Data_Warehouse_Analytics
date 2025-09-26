# Required libraries
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules

# ---------------------
# STEP 1: Load dimension tables (make sure they are in the same folder)
# ---------------------

# Load vehicle, driver, and location dimensions
vehicle_df = pd.read_csv("vehicle_dimension.csv")
driver_df = pd.read_csv("driver_dimension.csv")
location_df = pd.read_csv("location_dimension.csv")

# ---------------------
# STEP 2: Clean and prepare features
# ---------------------

# Assign vehicle type based on involvement flags
vehicle_df["vehicle_type"] = vehicle_df.apply(
    lambda row: "Articulated Truck" if row["Articulated Truck Involvement"] == 1
    else "Heavy Rigid Truck" if row["Heavy Rigid Truck Involvement"] == 1
    else "Bus" if row["Bus Involvement"] == 1
    else "Other", axis=1)

# Select relevant columns
vehicle_simple = vehicle_df[["crash_id", "vehicle_type"]]
driver_simple = driver_df[["crash_id", "gender", "age_group"]]
location_simple = location_df[["location_id", "state"]]

# Merge driver and vehicle info on crash_id
merged_df = pd.merge(driver_simple, vehicle_simple, on="crash_id", how="inner")

# Temporarily assign location_id (in real case, join from fact_table if available)
merged_df["location_id"] = merged_df.index + 1  # Fake mapping just for demo

# Merge location info
final_df = pd.merge(merged_df, location_simple, on="location_id", how="left")

# ---------------------
# STEP 3: Build transaction strings
# ---------------------

# Convert each row into a list of transaction features
final_df["transaction"] = final_df.apply(
    lambda row: [f"vehicle={row.vehicle_type}",
                 f"driver_age={row.age_group}",
                 f"gender={row.gender}",
                 f"state={row.state}"], axis=1)

# Preview
print("Sample Transactions:")
print(final_df[["crash_id", "transaction"]].head())

# ---------------------
# STEP 4: Export to CSV
# ---------------------

transaction_df = final_df[["crash_id", "transaction"]]
transaction_df.to_csv("traffic_transactions.csv", index=False)
print("✅ Transaction CSV exported as 'traffic_transactions.csv'.")

# ---------------------
# STEP 5: One-hot encode transactions
# ---------------------

transactions = transaction_df["transaction"].tolist()
te = TransactionEncoder()
te_array = te.fit(transactions).transform(transactions)
df_encoded = pd.DataFrame(te_array, columns=te.columns_)

# ---------------------
# STEP 6: Run Apriori and generate rules
# ---------------------

# Minimum support = 20%
frequent_itemsets = apriori(df_encoded, min_support=0.2, use_colnames=True)

# Generate rules using 'lift' as metric
rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1.0)

# Simplify result
rules_summary = rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']]
rules_sorted = rules_summary.sort_values(by='lift', ascending=False)

# ---------------------
# STEP 7: Output results
# ---------------------

print("\n📊 Top Association Rules (by lift):")
print(rules_sorted.head(10))

