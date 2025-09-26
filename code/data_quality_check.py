import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Create quality reports directory if it doesn't exist
QUALITY_DIR = 'quality_reports_2001_2023'
if not os.path.exists(QUALITY_DIR):
    os.makedirs(QUALITY_DIR)

print("Starting Data Quality Check Process...")

# Utility functions for quality checks
def calculate_null_percentage(df):
    """Calculate the percentage of null values in each column."""
    total_rows = len(df)
    null_counts = df.isnull().sum()
    null_percentage = (null_counts / total_rows * 100).round(2)
    return pd.DataFrame({'null_count': null_counts, 'null_percentage': null_percentage})

def check_duplicates(df, key_columns=None):
    """Check for duplicates in the dataframe."""
    if key_columns:
        duplicate_count = df.duplicated(subset=key_columns).sum()
        return {
            'duplicate_count': duplicate_count,
            'duplicate_percentage': round(duplicate_count / len(df) * 100, 2),
            'checked_columns': key_columns
        }
    else:
        duplicate_count = df.duplicated().sum()
        return {
            'duplicate_count': duplicate_count,
            'duplicate_percentage': round(duplicate_count / len(df) * 100, 2),
            'checked_columns': 'all columns'
        }

def check_value_distribution(df, column):
    """Check the distribution of values in a column."""
    if df[column].dtype in ['int64', 'float64']:
        return {
            'min': df[column].min(),
            'max': df[column].max(),
            'mean': df[column].mean(),
            'median': df[column].median(),
            'std': df[column].std()
        }
    else:
        value_counts = df[column].value_counts().head(10)
        return {
            'unique_values': df[column].nunique(),
            'top_values': value_counts.to_dict()
        }

def generate_report(df, dimension_name, primary_key=None):
    """Generate a comprehensive data quality report for a dimension."""
    report = []
    report.append(f"Data Quality Report for {dimension_name}")
    report.append("=" * 50)
    report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Records: {len(df)}")
    
    # Check for nulls
    report.append("\nNull Value Analysis:")
    report.append("-" * 30)
    null_analysis = calculate_null_percentage(df)
    for column, row in null_analysis.iterrows():
        if row['null_count'] > 0:
            report.append(f"{column}: {row['null_count']} nulls ({row['null_percentage']}%)")
    
    # Check for duplicates
    report.append("\nDuplicate Analysis:")
    report.append("-" * 30)
    if primary_key:
        dup_analysis = check_duplicates(df, [primary_key])
        report.append(f"Duplicate {primary_key}s: {dup_analysis['duplicate_count']} ({dup_analysis['duplicate_percentage']}%)")
    
    # Overall duplicate check
    dup_analysis = check_duplicates(df)
    report.append(f"Duplicate rows: {dup_analysis['duplicate_count']} ({dup_analysis['duplicate_percentage']}%)")
    
    # Column data quality checks
    report.append("\nColumn Analysis:")
    report.append("-" * 30)
    
    for column in df.columns:
        report.append(f"\n{column}:")
        report.append(f"  Data Type: {df[column].dtype}")
        report.append(f"  Unique Values: {df[column].nunique()}")
        
        if df[column].dtype in ['int64', 'float64']:
            stats = check_value_distribution(df, column)
            report.append(f"  Min: {stats['min']}")
            report.append(f"  Max: {stats['max']}")
            report.append(f"  Mean: {stats['mean']}")
            report.append(f"  Median: {stats['median']}")
        else:
            stats = check_value_distribution(df, column)
            report.append(f"  Top Values: {list(stats['top_values'].items())[:5]}")
    
    # Generate and save visual
    try:
        # Only create visuals for dataframes with reasonable size
        if len(df.columns) <= 20 and len(df) <= 100000:
            plt.figure(figsize=(10, 6))
            null_data = df.isnull().sum() / len(df) * 100
            null_data = null_data[null_data > 0].sort_values(ascending=False)
            
            if not null_data.empty:
                ax = sns.barplot(x=null_data.index, y=null_data.values)
                plt.title(f'Null Percentage in {dimension_name}')
                plt.ylabel('Null Percentage')
                plt.xlabel('Columns')
                plt.xticks(rotation=90)
                plt.tight_layout()
                plt.savefig(f"{QUALITY_DIR}/{dimension_name.lower().replace(' ', '_')}_nulls.png")
            plt.close()
    except Exception as e:
        report.append(f"\nError generating visual: {str(e)}")
    
    return "\n".join(report)

def check_referential_integrity(df1, df2, key, dim1_name, dim2_name):
    """Check referential integrity between two dataframes."""
    keys_in_df1 = set(df1[key].unique())
    keys_in_df2 = set(df2[key].unique())
    
    orphans_in_df2 = keys_in_df2 - keys_in_df1
    
    report = []
    report.append(f"Referential Integrity Check: {dim1_name} to {dim2_name}")
    report.append("=" * 50)
    report.append(f"Key field: {key}")
    report.append(f"Unique keys in {dim1_name}: {len(keys_in_df1)}")
    report.append(f"Unique keys in {dim2_name}: {len(keys_in_df2)}")
    report.append(f"Orphaned records in {dim2_name}: {len(orphans_in_df2)}")
    
    if len(orphans_in_df2) > 0:
        report.append(f"Sample of orphaned keys: {list(orphans_in_df2)[:5]}")
    
    return "\n".join(report)

# Load all dimension tables
print("Loading dimension tables...")
try:
    location_dim = pd.read_csv('output/location_dimension.csv')
    vehicle_dim = pd.read_csv('output/vehicle_dimension.csv')
    road_condition_dim = pd.read_csv('output/road_condition_dimension.csv')
    driver_dim = pd.read_csv('output/driver_dimension.csv')
    time_dim = pd.read_csv('output/time_dimension.csv')
    population_dim = pd.read_csv('output/population_dimension.csv')
    crash_type_dim = pd.read_csv('output/crash_type_dimension.csv')
    fact_table = pd.read_csv('output/fact_table.csv')
    
    print("All dimension tables loaded successfully")
except Exception as e:
    print(f"Error loading dimension tables: {e}")
    exit(1)

# Generate reports for each dimension
print("Generating quality reports...")

# Location Dimension
location_report = generate_report(location_dim, "Location Dimension", "Crash ID")
with open(f"{QUALITY_DIR}/location_dimension_report.txt", "w") as f:
    f.write(location_report)

# Vehicle Dimension
vehicle_report = generate_report(vehicle_dim, "Vehicle Dimension", "Crash ID")
with open(f"{QUALITY_DIR}/vehicle_dimension_report.txt", "w") as f:
    f.write(vehicle_report)

# Road Condition Dimension
road_report = generate_report(road_condition_dim, "Road Condition Dimension", "Crash ID")
with open(f"{QUALITY_DIR}/road_condition_dimension_report.txt", "w") as f:
    f.write(road_report)

# Driver Dimension
driver_report = generate_report(driver_dim, "Driver Dimension", "Crash ID")
with open(f"{QUALITY_DIR}/driver_dimension_report.txt", "w") as f:
    f.write(driver_report)

# Time Dimension
time_report = generate_report(time_dim, "Time Dimension", "Crash ID")
with open(f"{QUALITY_DIR}/time_dimension_report.txt", "w") as f:
    f.write(time_report)

# Population Dimension
population_report = generate_report(population_dim, "Population Dimension")
with open(f"{QUALITY_DIR}/population_dimension_report.txt", "w") as f:
    f.write(population_report)

# Crash Type Dimension
crash_type_report = generate_report(crash_type_dim, "Crash Type Dimension", "Crash ID")
with open(f"{QUALITY_DIR}/crash_type_dimension_report.txt", "w") as f:
    f.write(crash_type_report)

# Fact Table
fact_report = generate_report(fact_table, "Fact Table", "Crash ID")
with open(f"{QUALITY_DIR}/fact_table_report.txt", "w") as f:
    f.write(fact_report)

# Check time validity
print("Checking time validity...")
time_check = []
time_check.append("Time Validity Check")
time_check.append("=" * 50)

# Check if all years are within 2001-2023
year_check = time_dim[(time_dim['Year'] < 2001) | (time_dim['Year'] > 2023)]
time_check.append(f"Records outside 2001-2023 range: {len(year_check)}")

# Check if date format is consistent
if pd.api.types.is_datetime64_any_dtype(time_dim['Date']):
    time_check.append("Date format: Valid datetime format")
else:
    try:
        pd.to_datetime(time_dim['Date'])
        time_check.append("Date format: Can be converted to datetime")
    except:
        time_check.append("Date format: INVALID - cannot be converted to datetime")

# Check time format for issues
time_format_issues = time_dim[~time_dim['Time'].str.match(r'^\d{2}:\d{2}$', na=True)]
time_check.append(f"Records with time format issues: {len(time_format_issues)}")
if len(time_format_issues) > 0:
    time_check.append(f"Sample time format issues: {time_format_issues['Time'].head(5).tolist()}")

with open(f"{QUALITY_DIR}/time_validity_check.txt", "w") as f:
    f.write("\n".join(time_check))

# Check referential integrity
print("Checking referential integrity...")

# Check integrity between fact table and dimensions
integrity_checks = []

# Fact Table to Location Dimension
integrity_checks.append(check_referential_integrity(
    location_dim, fact_table, "Crash ID", "Location Dimension", "Fact Table"))

# Fact Table to Vehicle Dimension
integrity_checks.append(check_referential_integrity(
    vehicle_dim, fact_table, "Crash ID", "Vehicle Dimension", "Fact Table"))

# Fact Table to Time Dimension
integrity_checks.append(check_referential_integrity(
    time_dim, fact_table, "Crash ID", "Time Dimension", "Fact Table"))

# Fact Table to Crash Type Dimension
integrity_checks.append(check_referential_integrity(
    crash_type_dim, fact_table, "Crash ID", "Crash Type Dimension", "Fact Table"))

with open(f"{QUALITY_DIR}/referential_integrity_report.txt", "w") as f:
    f.write("\n\n".join(integrity_checks))

# Generate year distribution analysis
print("Analyzing year distribution...")
plt.figure(figsize=(12, 6))
year_counts = fact_table['Year'].value_counts().sort_index()
sns.barplot(x=year_counts.index, y=year_counts.values)
plt.title('Distribution of Records by Year (2001-2023)')
plt.xlabel('Year')
plt.ylabel('Number of Records')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(f"{QUALITY_DIR}/year_distribution.png")
plt.close()

# Generate summary report
print("Generating summary report...")
summary = []
summary.append("Data Quality Summary Report for 2001-2023 Data")
summary.append("=" * 60)
summary.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
summary.append(f"Total Crash Records: {len(fact_table)}")
summary.append(f"Year Range: 2001-2023")
summary.append(f"Total Driver Records: {len(driver_dim)}")

# Count null values across all dimensions
all_dims = {
    "Location": location_dim,
    "Vehicle": vehicle_dim,
    "Road": road_condition_dim,
    "Driver": driver_dim,
    "Time": time_dim,
    "Crash Type": crash_type_dim,
    "Fact Table": fact_table
}

summary.append("\nNull Values Summary:")
summary.append("-" * 30)

for name, dim in all_dims.items():
    null_count = dim.isnull().sum().sum()
    null_percent = round(null_count / (dim.shape[0] * dim.shape[1]) * 100, 2)
    summary.append(f"{name}: {null_count} nulls ({null_percent}% of data)")

# Summary of data validation checks
summary.append("\nData Validation Summary:")
summary.append("-" * 30)

year_check = time_dim[(time_dim['Year'] < 2001) | (time_dim['Year'] > 2023)]
summary.append(f"Records outside 2001-2023 range: {len(year_check)}")

time_format_issues = time_dim[~time_dim['Time'].str.match(r'^\d{2}:\d{2}$', na=True)]
summary.append(f"Records with time format issues: {len(time_format_issues)}")

# Add most common values in important dimensions
summary.append("\nCommon Values in Key Dimensions:")
summary.append("-" * 30)

summary.append(f"Top States: {fact_table['State'].value_counts().head(3).to_dict()}")
summary.append(f"Year with Most Records: {fact_table['Year'].value_counts().idxmax()} ({fact_table['Year'].value_counts().max()} records)")
summary.append(f"Month with Most Records: {fact_table['Month'].value_counts().idxmax()} ({fact_table['Month'].value_counts().max()} records)")

with open(f"{QUALITY_DIR}/summary_report.txt", "w") as f:
    f.write("\n".join(summary))

print(f"Data quality check completed. Reports saved to {QUALITY_DIR}/") 