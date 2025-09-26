import pandas as pd
import numpy as np
import os

print("Starting Driver Age Data Fix Process...")

# Load driver dimension file
print("Loading driver dimension data...")
driver_dim = pd.read_csv('output/driver_dimension.csv')

# Analyze missing age values
null_age_records = driver_dim[driver_dim['Age'].isnull()]
print(f"Found {len(null_age_records)} records with missing age values")

if len(null_age_records) > 0:
    print("\nSample records with missing age:")
    print(null_age_records.head())
    
    # Check if Age Group is available for records with missing Age
    has_age_group = null_age_records['Age Group'].notna().sum()
    print(f"\nRecords with missing Age but having Age Group: {has_age_group}/{len(null_age_records)}")
    
    # Create a backup
    backup_dir = 'output/backup'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    os.system('cp output/driver_dimension.csv output/backup/driver_dimension_backup.csv')
    print("Backup created at output/backup/driver_dimension_backup.csv")
    
    # Fix missing age values using Age Group
    print("\nFilling missing age values based on Age Group...")
    
    # Create a mapping from Age Group to median age
    age_group_mapping = {}
    
    # Calculate median age for each age group from existing data
    for age_group in driver_dim['Age Group'].dropna().unique():
        if age_group != 'Unknown':
            median_age = driver_dim[driver_dim['Age Group'] == age_group]['Age'].median()
            age_group_mapping[age_group] = median_age
    
    print("\nMedian age by Age Group:")
    for group, median in age_group_mapping.items():
        print(f"{group}: {median}")
    
    # Function to map age group to median age
    def get_median_age(age_group):
        if pd.isna(age_group) or age_group == 'Unknown':
            return driver_dim['Age'].median()  # Overall median
        return age_group_mapping.get(age_group, driver_dim['Age'].median())
    
    # Fill missing ages
    driver_dim.loc[driver_dim['Age'].isnull(), 'Age'] = driver_dim.loc[driver_dim['Age'].isnull(), 'Age Group'].apply(get_median_age)
    
    # For any remaining NaN values, use overall median
    overall_median = driver_dim['Age'].median()
    driver_dim['Age'].fillna(overall_median, inplace=True)
    
    # Save the updated file
    driver_dim.to_csv('output/driver_dimension.csv', index=False)
    print("\nFixed driver dimension saved to output/driver_dimension.csv")
    
    # Verify the fix
    remaining_nulls = driver_dim['Age'].isnull().sum()
    print(f"\nRemaining null age values: {remaining_nulls}")
else:
    print("No missing age values found in driver dimension.")

print("\nDriver age data fix process completed.") 