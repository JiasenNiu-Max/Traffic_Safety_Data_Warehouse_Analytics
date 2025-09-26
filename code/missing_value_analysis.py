import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns

print("Starting Missing Value Analysis...")

# Load location dimension table
print("Loading location dimension data...")
location_dim = pd.read_csv('output/location_dimension.csv')

# Analyze missing values
print("\nMissing Value Analysis for Location Dimension:")
null_counts = location_dim.isnull().sum()
null_percentages = (null_counts / len(location_dim) * 100).round(2)

print("\nNull counts by column:")
for col, count in null_counts.items():
    if count > 0:
        print(f"{col}: {count} nulls ({null_percentages[col]}%)")

# Visualize missing data
plt.figure(figsize=(10, 6))
sns.heatmap(location_dim.isnull(), yticklabels=False, cbar=False, cmap='viridis')
plt.title('Missing Value Heatmap (Location Dimension)')
plt.tight_layout()
plt.savefig('missing_values_heatmap.png')
plt.close()

# Create a backup before modifying
backup_dir = 'output/backup'
if not os.path.exists(backup_dir):
    os.makedirs(backup_dir)

os.system('cp output/location_dimension.csv output/backup/location_dimension_backup_before_fix.csv')
print("Backup created at output/backup/location_dimension_backup_before_fix.csv")

# Check if SA4 Name and LGA Name are missing together
print("\nChecking patterns in missing data...")
both_missing = location_dim[location_dim['SA4 Name 2021'].isnull() & location_dim['National LGA Name 2021'].isnull()]
print(f"Records missing both SA4 and LGA names: {len(both_missing)} ({len(both_missing)/len(location_dim)*100:.2f}%)")

# Analyze by State and Remoteness Areas
print("\nMissing data by State:")
# Create a mask to identify missing values
sa4_missing_mask = location_dim['SA4 Name 2021'].isnull()

# Group by state and calculate missing percentage
state_groups = location_dim.groupby('State')
for state, group in state_groups:
    missing_count = group['SA4 Name 2021'].isnull().sum()
    total_count = len(group)
    missing_percent = (missing_count / total_count * 100).round(2)
    print(f"{state}: {missing_count}/{total_count} records ({missing_percent}%) missing SA4 data")

# Analyze by Remoteness Areas
print("\nMissing data by Remoteness Area:")
# Group by remoteness area and calculate missing percentage
remote_groups = location_dim.groupby('National Remoteness Areas')
for area, group in remote_groups:
    if pd.isna(area):
        continue
    missing_count = group['SA4 Name 2021'].isnull().sum()
    total_count = len(group)
    missing_percent = (missing_count / total_count * 100).round(2)
    print(f"{area}: {missing_count}/{total_count} records ({missing_percent}%) missing SA4 data")

# Fill missing data with meaningful values
print("\nHandling missing values...")

# Option 1: Fill with 'Unknown' for text fields
location_dim['SA4 Name 2021'].fillna('Unknown', inplace=True)
location_dim['National LGA Name 2021'].fillna('Unknown', inplace=True)

# Save the fixed file
location_dim.to_csv('output/location_dimension.csv', index=False)
print("Fixed location dimension saved to output/location_dimension.csv")

# Verify the fix
fixed_nulls = location_dim.isnull().sum()
print("\nRemaining null values after fix:")
for col, count in fixed_nulls.items():
    if count > 0:
        print(f"{col}: {count} nulls")
    else:
        print(f"{col}: No nulls")

print("\nMissing value handling completed.")

# Optional: Look for other dimensions with significant missing data
print("\nChecking other dimensions for missing data...")

# Load other dimension tables
vehicle_dim = pd.read_csv('output/vehicle_dimension.csv')
road_condition_dim = pd.read_csv('output/road_condition_dimension.csv')
driver_dim = pd.read_csv('output/driver_dimension.csv')
time_dim = pd.read_csv('output/time_dimension.csv')
crash_type_dim = pd.read_csv('output/crash_type_dimension.csv')

# Create a summary of missing values across all dimensions
dimensions = {
    'Location': location_dim,
    'Vehicle': vehicle_dim,
    'Road Condition': road_condition_dim,
    'Driver': driver_dim,
    'Time': time_dim,
    'Crash Type': crash_type_dim
}

print("\nMissing value summary for all dimensions:")
for name, df in dimensions.items():
    total_nulls = df.isnull().sum().sum()
    total_cells = df.size
    percent_nulls = (total_nulls / total_cells * 100).round(2)
    print(f"{name}: {total_nulls} nulls ({percent_nulls}% of data)")
    
    # Show details for dimensions with missing values
    if total_nulls > 0:
        null_cols = df.columns[df.isnull().any()].tolist()
        print(f"  Columns with nulls: {null_cols}")
        for col in null_cols:
            null_count = df[col].isnull().sum()
            null_percent = (null_count / len(df) * 100).round(2)
            print(f"    {col}: {null_count} nulls ({null_percent}%)")

print("\nMissing value analysis completed.") 