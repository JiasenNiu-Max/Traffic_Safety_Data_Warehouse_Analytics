import pandas as pd
import os

print("Starting time data fix process...")

# Load time dimension file
print("Loading time dimension data...")
time_dim = pd.read_csv('output/time_dimension.csv')

# Display problematic records
problem_records = time_dim[~time_dim['Time'].str.match(r'^\d{2}:\d{2}$', na=True)]
print(f"Found {len(problem_records)} records with time format issues")
print("Sample problem records:")
print(problem_records[['Crash ID', 'Time']].head())

# Fix time format issues
print("Fixing time format issues...")
# Set problematic times to a default value (e.g., "00:00")
time_dim.loc[~time_dim['Time'].str.match(r'^\d{2}:\d{2}$', na=True), 'Time'] = "00:00"

# Save fixed time dimension
print("Saving fixed time dimension data...")
backup_dir = 'output/backup'
if not os.path.exists(backup_dir):
    os.makedirs(backup_dir)
    
# Create backup
if os.path.exists('output/time_dimension.csv'):
    # Create backup
    os.system('cp output/time_dimension.csv output/backup/time_dimension_backup.csv')
    print("Backup created at output/backup/time_dimension_backup.csv")

# Save fixed data
time_dim.to_csv('output/time_dimension.csv', index=False)
print("Fixed time dimension saved to output/time_dimension.csv")

# Also update the fact table if it has any time-related issues
print("Checking fact table for date-related issues...")
fact_table = pd.read_csv('output/fact_table.csv')

# Ensure Date is in proper datetime format
if 'Date' in fact_table.columns:
    # Convert to datetime
    fact_table['Date'] = pd.to_datetime(fact_table['Date']).dt.strftime('%Y-%m-%d')
    
    # Create backup
    os.system('cp output/fact_table.csv output/backup/fact_table_backup.csv')
    print("Backup created at output/backup/fact_table_backup.csv")
    
    # Save fixed fact table
    fact_table.to_csv('output/fact_table.csv', index=False)
    print("Fixed fact table saved to output/fact_table.csv")

print("Time data fix process completed.") 