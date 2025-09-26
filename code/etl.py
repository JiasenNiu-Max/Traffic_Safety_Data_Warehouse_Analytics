import pandas as pd
import numpy as np
import os
import traceback
from datetime import datetime

# Create output directory if it doesn't exist
if not os.path.exists('output'):
    os.makedirs('output')

print("Starting Enhanced ETL process with improved population data...")

# Function to replace -9 values with NaN
def clean_missing_values(df):
    return df.replace(-9, np.nan)

# Function to clean column names (trim whitespace and newlines)
def clean_column_names(df):
    df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
    return df

# Function to get season from month
def get_season(month):
    if month in [12, 1, 2]:  # Australian summer
        return "Summer"
    elif month in [3, 4, 5]:  # Australian autumn
        return "Autumn"
    elif month in [6, 7, 8]:  # Australian winter
        return "Winter"
    else:  # Months 9, 10, 11, Australian spring
        return "Spring"

# Load CSV files
print("Loading CSV files...")
try:
    # Load the crash data
    fatal_crash = pd.read_csv('data/bitre_fatal_crash.csv', skiprows=4, low_memory=False)
    fatal_crash = clean_column_names(fatal_crash)
    
    fatalities = pd.read_csv('data/bitre_fatalities.csv', skiprows=4, low_memory=False)
    fatalities = clean_column_names(fatalities)
    
    # Load count data
    fatal_crash_count = pd.read_csv('data/bitre_fatal_crash_count_by_date.csv', skiprows=4, low_memory=False)
    fatal_crash_count = clean_column_names(fatal_crash_count)
    
    fatalities_count = pd.read_csv('data/bitre_fatalities_count_by_date.csv', skiprows=4, low_memory=False)
    fatalities_count = clean_column_names(fatalities_count)
    
    # Load location reference data
    print("Loading location reference data...")
    
    # Load local government areas data (reading manually to handle complex structure)
    print("Processing local government areas data...")
    lga_file = 'data/local_government_areas.csv'
    
    # First pass to find where data starts
    with open(lga_file, 'r') as f:
        lines = f.readlines()
    
    # Find the header row
    header_row = -1
    data_rows = []
    
    for i, line in enumerate(lines):
        if 'LGA' in line and 'Code' in line and 'Name' in line:
            header_row = i
            break
    
    if header_row >= 0:
        # Extract relevant columns manually
        lga_data = []
        
        # Define column indices based on inspection
        code_idx = 0
        name_idx = 1
        population_idx = 2
        
        # Process lines following header
        for i in range(header_row + 1, len(lines)):
            line = lines[i].strip().split(',')
            if len(line) > population_idx:
                try:
                    code = line[code_idx].strip()
                    name = line[name_idx].strip()
                    population = line[population_idx].strip()
                    
                    # Only include rows with valid data
                    if code and name and population and population.replace('.', '', 1).isdigit():
                        lga_data.append({
                            'LGA_Code': code,
                            'LGA_Name': name,
                            'LGA_Population': float(population)
                        })
                except:
                    continue
        
        lga_df = pd.DataFrame(lga_data)
        print(f"Extracted {len(lga_df)} LGA records with population data")
    else:
        print("Could not find LGA header row in the file")
        lga_df = pd.DataFrame()
    
    # Load remoteness areas data
    print("Processing remoteness areas data...")
    remoteness_file = 'data/remoteness_areas.csv'
    
    # Define the remoteness categories and states to match
    remoteness_categories = [
        'Major Cities', 
        'Inner Regional', 
        'Outer Regional', 
        'Remote', 
        'Very Remote'
    ]
    
    states = ['NSW', 'Vic', 'Qld', 'SA', 'WA', 'Tas', 'NT', 'ACT']
    
    # Read the file manually to handle the complex structure
    with open(remoteness_file, 'r') as f:
        lines = f.readlines()
    
    # Find remoteness data
    remoteness_data = []
    state_indices = {}
    
    # First identify where the data for each state begins
    for i, line in enumerate(lines):
        for state in states:
            if state in line and 'Australia' in line:
                state_indices[state] = i
                break
    
    # Process data for each state
    for state, start_idx in state_indices.items():
        for category in remoteness_categories:
            # Look for the category in lines near the state start index
            for i in range(start_idx, min(start_idx + 10, len(lines))):
                if category in lines[i]:
                    try:
                        # Extract population data
                        values = lines[i].strip().split(',')
                        for value in values:
                            value = value.strip()
                            # Check if the value looks like a population count
                            if value and value.replace('.', '', 1).isdigit():
                                area_name = f"{category} Australia ({state})"
                                remoteness_data.append({
                                    'State': state,
                                    'Remoteness_Category': category,
                                    'Remoteness_Area': area_name,
                                    'Population': float(value)
                                })
                                break
                    except Exception as e:
                        print(f"Error processing remoteness data for {state}, {category}: {e}")
                    break
    
    remoteness_df = pd.DataFrame(remoteness_data)
    print(f"Extracted {len(remoteness_df)} remoteness area records with population data")
    
    print("CSV and reference data files loaded successfully")
    print(f"Fatal crash columns: {fatal_crash.columns.tolist()}")
    print(f"Fatalities columns: {fatalities.columns.tolist()}")
    
except Exception as e:
    print(f"Error loading files: {e}")
    print(traceback.format_exc())
    exit(1)

# Clean data - replace -9 with NaN
fatal_crash = clean_missing_values(fatal_crash)
fatalities = clean_missing_values(fatalities)

# Standardize date and time data and filter by year range (2001-2023)
print("Standardizing time data and filtering for years 2001-2023...")

# Ensure Year is numeric
fatal_crash['Year'] = pd.to_numeric(fatal_crash['Year'], errors='coerce')
fatalities['Year'] = pd.to_numeric(fatalities['Year'], errors='coerce')

# Filter for years 2001-2023
fatal_crash = fatal_crash[(fatal_crash['Year'] >= 2001) & (fatal_crash['Year'] <= 2023)]
fatalities = fatalities[(fatalities['Year'] >= 2001) & (fatalities['Year'] <= 2023)]

print(f"After filtering: {len(fatal_crash)} crash records and {len(fatalities)} fatality records")

# Create standardized date column
fatal_crash['Date'] = pd.to_datetime(
    fatal_crash['Year'].astype(str) + '-' + 
    fatal_crash['Month'].astype(str).str.zfill(2) + '-01'
)

# Standardize time format if needed
if 'Time' in fatal_crash.columns:
    # Convert time to standard format if it's not already
    try:
        # Try to standardize time format if it's in a string format
        if fatal_crash['Time'].dtype == 'object':
            # First ensure it's a string
            fatal_crash['Time'] = fatal_crash['Time'].astype(str)
            # Remove any existing colons
            fatal_crash['Time'] = fatal_crash['Time'].str.replace(':', '')
            # Pad with leading zeros to ensure 4 digits
            fatal_crash['Time'] = fatal_crash['Time'].str.zfill(4)
            # Insert colon between hours and minutes
            fatal_crash['Time'] = fatal_crash['Time'].str[:2] + ':' + fatal_crash['Time'].str[2:4]
    except Exception as e:
        print(f"Warning: Could not standardize time format: {e}")

# Create filtered fatalities dataset based on the crash IDs from filtered fatal_crash
valid_crash_ids = set(fatal_crash['Crash ID'].unique())
fatalities = fatalities[fatalities['Crash ID'].isin(valid_crash_ids)]

# 1. Location Dimension
print("Creating Location Dimension...")
location_dim = pd.DataFrame()
location_dim['Crash ID'] = fatal_crash['Crash ID']
location_dim['State'] = fatal_crash['State']
location_dim['National Remoteness Areas'] = fatal_crash['National Remoteness Areas']
location_dim['SA4 Name 2021'] = fatal_crash['SA4 Name 2021']
location_dim['National LGA Name 2021'] = fatal_crash['National LGA Name 2021']

# Fill missing values with 'Unknown'
location_dim['SA4 Name 2021'] = location_dim['SA4 Name 2021'].fillna('Unknown')
location_dim['National LGA Name 2021'] = location_dim['National LGA Name 2021'].fillna('Unknown')

# Remove duplicates
location_dim = location_dim.drop_duplicates()
location_dim.to_csv('output/location_dimension.csv', index=False)
print(f"Location Dimension created with {len(location_dim)} records")

# 2. Vehicle Dimension
print("Creating Vehicle Dimension...")
vehicle_dim = pd.DataFrame()
vehicle_dim['Crash ID'] = fatal_crash['Crash ID']

# Check if the columns exist before adding them
for col in ['Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement']:
    if col in fatal_crash.columns:
        vehicle_dim[col] = fatal_crash[col]
    # Check for alternate names
    elif 'Bus  Involvement' in fatal_crash.columns and col == 'Bus Involvement':
        vehicle_dim[col] = fatal_crash['Bus  Involvement']

# Remove duplicates
vehicle_dim = vehicle_dim.drop_duplicates()
vehicle_dim.to_csv('output/vehicle_dimension.csv', index=False)
print(f"Vehicle Dimension created with {len(vehicle_dim)} records")

# 3. Road Condition Dimension
print("Creating Road Condition Dimension...")
road_condition_dim = pd.DataFrame()
road_condition_dim['Crash ID'] = fatal_crash['Crash ID']
road_condition_dim['Speed Limit'] = fatal_crash['Speed Limit']
road_condition_dim['National Road Type'] = fatal_crash['National Road Type']

# Remove duplicates
road_condition_dim = road_condition_dim.drop_duplicates()
road_condition_dim.to_csv('output/road_condition_dimension.csv', index=False)
print(f"Road Condition Dimension created with {len(road_condition_dim)} records")

# 4. Driver Dimension
print("Creating Driver Dimension...")
driver_dim = pd.DataFrame()
driver_dim['Crash ID'] = fatalities['Crash ID']
driver_dim['Road User'] = fatalities['Road User']
driver_dim['Gender'] = fatalities['Gender']
driver_dim['Age'] = fatalities['Age']
driver_dim['Age Group'] = fatalities['Age Group']

# Fix missing age values
if driver_dim['Age'].isnull().sum() > 0:
    print(f"Found {driver_dim['Age'].isnull().sum()} records with missing age values")
    
    # Create a mapping from Age Group to median age
    age_group_mapping = {}
    
    # Calculate median age for each age group from existing data
    for age_group in driver_dim['Age Group'].dropna().unique():
        if age_group != 'Unknown' and age_group != '-9':
            median_age = driver_dim[driver_dim['Age Group'] == age_group]['Age'].median()
            age_group_mapping[age_group] = median_age
    
    # Function to map age group to median age
    def get_median_age(age_group):
        if pd.isna(age_group) or age_group == 'Unknown' or age_group == '-9':
            return driver_dim['Age'].median()  # Overall median
        return age_group_mapping.get(age_group, driver_dim['Age'].median())
    
    # Fill missing ages
    driver_dim.loc[driver_dim['Age'].isnull(), 'Age'] = driver_dim.loc[driver_dim['Age'].isnull(), 'Age Group'].apply(get_median_age)
    
    # For any remaining NaN values, use overall median
    overall_median = driver_dim['Age'].median()
    driver_dim['Age'] = driver_dim['Age'].fillna(overall_median)
    
    print(f"Fixed age values. Remaining null values: {driver_dim['Age'].isnull().sum()}")

# Remove duplicates
driver_dim = driver_dim.drop_duplicates()
driver_dim.to_csv('output/driver_dimension.csv', index=False)
print(f"Driver Dimension created with {len(driver_dim)} records")

# 5. Time Dimension
print("Creating Time Dimension...")
time_dim = pd.DataFrame()
time_dim['Crash ID'] = fatal_crash['Crash ID']
time_dim['Month'] = fatal_crash['Month']
time_dim['Year'] = fatal_crash['Year']
time_dim['Dayweek'] = fatal_crash['Dayweek']
time_dim['Time'] = fatal_crash['Time']
time_dim['Day of week'] = fatal_crash['Day of week']
time_dim['Time of Day'] = fatal_crash['Time of Day']
time_dim['Christmas Period'] = fatal_crash['Christmas Period']
time_dim['Easter Period'] = fatal_crash['Easter Period']
time_dim['Date'] = fatal_crash['Date']  # Add standardized date

# Remove duplicates
time_dim = time_dim.drop_duplicates()
time_dim.to_csv('output/time_dimension.csv', index=False)
print(f"Time Dimension created with {len(time_dim)} records")

# 6. Enhanced Population Dimension
print("Creating Enhanced Population Dimension...")
# Create the population dimension
population_dim = pd.DataFrame()

# Extract unique remoteness areas and LGAs from crash data
unique_remoteness = fatal_crash['National Remoteness Areas'].dropna().unique()
unique_lgas = fatal_crash['National LGA Name 2021'].dropna().unique()

# Process remoteness areas first
remoteness_entries = []

if not remoteness_df.empty:
    # Create a mapping table from crash data remoteness areas to reference data
    mapping = {}
    
    # For each remoteness area in the crash data
    for area in unique_remoteness:
        if not isinstance(area, str):
            continue
            
        # Try to find a match in the reference data
        best_match = None
        for _, row in remoteness_df.iterrows():
            ref_area = row['Remoteness_Area']
            # Try exact match
            if area == ref_area:
                best_match = row
                break
            # Try partial match
            elif isinstance(ref_area, str) and ref_area in area:
                best_match = row
                break
            # Try matching category and state
            elif (isinstance(row['Remoteness_Category'], str) and 
                 isinstance(row['State'], str) and 
                 row['Remoteness_Category'] in area and 
                 row['State'] in area):
                best_match = row
        
        if best_match is not None:
            mapping[area] = best_match
    
    # Create entries for the dimension table
    for area in unique_remoteness:
        if not isinstance(area, str):
            continue
            
        # Get state from area name
        state = 'Unknown'
        for s in states:
            if s in area:
                state = s
                break
                
        # Try to get population from mapping
        population = None
        if area in mapping:
            population = mapping[area]['Population']
            
        # Add entry
        remoteness_entries.append({
            'Remoteness Area': area,
            'State': state,
            'Population': population,
            'Data Source': 'Reference Data' if population is not None else 'Derived'
        })
else:
    # Create simple entries without population data
    for area in unique_remoteness:
        if isinstance(area, str):
            # Get state from area name
            state = 'Unknown'
            for s in states:
                if s in area:
                    state = s
                    break
                    
            remoteness_entries.append({
                'Remoteness Area': area,
                'State': state,
                'Population': None,
                'Data Source': 'Derived'
            })

# Create LGA entries
lga_entries = []

if not lga_df.empty:
    # For each LGA in the crash data, try to find a match in the reference data
    for lga in unique_lgas:
        if not isinstance(lga, str) or lga == 'Unknown':
            continue
            
        # Look for a matching LGA in the reference data
        match = lga_df[lga_df['LGA_Name'].str.contains(lga, na=False)]
        
        if not match.empty:
            lga_entries.append({
                'LGA Name': lga,
                'Population': match.iloc[0]['LGA_Population'],
                'Data Source': 'Reference Data'
            })
        else:
            lga_entries.append({
                'LGA Name': lga,
                'Population': None,
                'Data Source': 'Derived'
            })
else:
    # Create simple LGA entries without population data
    for lga in unique_lgas:
        if isinstance(lga, str) and lga != 'Unknown':
            lga_entries.append({
                'LGA Name': lga,
                'Population': None,
                'Data Source': 'Derived'
            })

# Create the population dimension with remoteness areas
if remoteness_entries:
    population_dim = pd.DataFrame(remoteness_entries)
    print(f"Created Population Dimension with {len(population_dim)} remoteness area records")
else:
    # Fallback to basic population dimension
    population_areas_by_state = fatal_crash[['State', 'National Remoteness Areas']].dropna().drop_duplicates()
    population_areas_by_state.columns = ['State', 'Remoteness Area']
    population_dim = population_areas_by_state
    print(f"Created basic Population Dimension with {len(population_dim)} records")

# Save LGA dimension separately if we have data
if lga_entries:
    lga_dim = pd.DataFrame(lga_entries)
    lga_dim.to_csv('output/lga_dimension.csv', index=False)
    print(f"Created LGA Dimension with {len(lga_dim)} records")

# Save the population dimension
population_dim.to_csv('output/population_dimension.csv', index=False)
print(f"Population Dimension saved with {len(population_dim)} records")

# 7. Crash Type Dimension
print("Creating Crash Type Dimension...")
crash_type_dim = pd.DataFrame()
crash_type_dim['Crash ID'] = fatal_crash['Crash ID']
crash_type_dim['Crash Type'] = fatal_crash['Crash Type']
crash_type_dim['Number Fatalities'] = fatal_crash['Number Fatalities']

# Remove duplicates
crash_type_dim = crash_type_dim.drop_duplicates()
crash_type_dim.to_csv('output/crash_type_dimension.csv', index=False)
print(f"Crash Type Dimension created with {len(crash_type_dim)} records")

# 8. Season Dimension
print("Creating Season Dimension...")
# Create a temporary table of unique year-month combinations
year_month_combinations = fatal_crash[['Year', 'Month']].drop_duplicates()
print(f"Found {len(year_month_combinations)} unique year-month combinations")

# Create season dimension
season_rows = []
for _, row in year_month_combinations.iterrows():
    year = row['Year']
    month = row['Month']
    
    # Build full date (first day of month)
    date_str = f"{year}-{month:02d}-01"
    
    # Determine season
    season = get_season(month)
    
    # Determine quarter
    quarter = (month - 1) // 3 + 1
    
    # Holiday season flag
    is_holiday_season = 1 if month in [11, 12, 1] else 0  # November to January as holiday season
    
    # School holiday flag (Australian school holidays typically in January, April, July, September-October)
    is_school_holiday = 1 if month in [1, 4, 7, 9, 10] else 0
    
    # Determine tourism season (based on Australian tourism peak periods)
    if month in [12, 1, 2]:  # Summer
        tourism_season = "Peak"
    elif month in [6, 7]:  # Winter school holidays
        tourism_season = "Secondary Peak"
    else:
        tourism_season = "Off Peak"
    
    # Add to rows list
    season_rows.append({
        'Year': year,
        'Month': month,
        'Season': season,
        'Quarter': quarter,
        'Is_Holiday_Season': is_holiday_season,
        'Is_School_Holiday': is_school_holiday,
        'Tourism_Season': tourism_season,
        'Date': date_str
    })

# Create DataFrame
season_dim = pd.DataFrame(season_rows)

# Add Season_ID as a unique identifier
season_dim['Season_ID'] = season_dim['Year'].astype(str) + '_' + season_dim['Quarter'].astype(str)

# Save season dimension
season_dim.to_csv('output/season_dimension.csv', index=False)
print(f"Season Dimension created with {len(season_dim)} records")

# 9. Fact Table
print("Creating Fact Table...")
fact_table = pd.DataFrame()
fact_table['Crash ID'] = fatal_crash['Crash ID']
fact_table['State'] = fatal_crash['State']
fact_table['Number Fatalities'] = fatal_crash['Number Fatalities']
fact_table['Year'] = fatal_crash['Year']
fact_table['Month'] = fatal_crash['Month']
fact_table['Date'] = fatal_crash['Date']  # Add standardized date

# Merge with counts if needed
# Create a date column for the year-month
fatal_crash['Year-Month'] = fatal_crash['Year'].astype(str) + '-' + fatal_crash['Month'].astype(str).str.zfill(2)

# Group by year-month to get counts
monthly_counts = fatal_crash.groupby('Year-Month').agg(
    Monthly_Crash_Count=('Crash ID', 'count'),
    Monthly_Fatality_Count=('Number Fatalities', 'sum')
).reset_index()

# Merge with fact table
fact_table = fact_table.merge(
    fatal_crash[['Crash ID', 'Year-Month']], 
    on='Crash ID',
    how='left'
)

fact_table = fact_table.merge(
    monthly_counts,
    on='Year-Month',
    how='left'
)

# Add season information to fact table
season_lookup = season_dim[['Year', 'Month', 'Season', 'Season_ID']].copy()
fact_table = fact_table.merge(
    season_lookup,
    on=['Year', 'Month'],
    how='left'
)

# Drop temporary column
fact_table = fact_table.drop('Year-Month', axis=1)

# Remove duplicates
fact_table = fact_table.drop_duplicates()
fact_table.to_csv('output/fact_table.csv', index=False)
print(f"Fact Table created with {len(fact_table)} records")

print("Enhanced ETL process completed successfully. Data filtered for years 2001-2023. Dimension tables saved to 'output' directory.") 