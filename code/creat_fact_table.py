import pandas as pd
import os

def create_fact_table():
    try:
        print("creat...")

        # 1. Unified function for loading CSV, automatically converting column names to lowercase
        def read_csv(path):
            df = pd.read_csv(path)
            df.columns = df.columns.str.lower()
            return df

        # 2.Loading dimension tables
        crash_df = read_csv("output/crash_type_dimension.csv")
        road_df = read_csv("output/road_condition_dimension.csv")
        vehicle_df = read_csv("output/vehicle_dimension.csv")
        driver_df = read_csv("output/driver_dimension.csv")
        location_df = read_csv("output/location_dimension.csv")
        population_df = read_csv("output/population_dimension.csv")
        season_df = read_csv("output/season_dimension.csv")
        lga_df = read_csv("output/lga_dimension.csv")
        
        # 3. Build the main table fact_df
        print("fact_table
              ...")
        crash_df.rename(columns={"number fatalities": "fatalities"}, inplace=True)
        fact_df = crash_df.copy()
        fact_df.insert(0, "fact_id", range(1, len(fact_df) + 1))
        fact_df["year"] = fact_df["crash_id"].astype(str).str[:4].astype(int)
        fact_df["state"] = fact_df["crash_id"].astype(str).str[4].map({
            '1': 'NSW', '2': 'VIC', '3': 'QLD', '4': 'SA',
            '5': 'WA', '6': 'TAS', '7': 'NT', '8': 'ACT'
        })

        # 4. Construct time_id & merge season_id
        print("construct time_id & merge season_id...")
        if 'month' not in season_df.columns:
            raise ValueError("❌ season_dimension.csv 中缺少 'month' 字段，请检查列名是否为 'Month' 或统一为小写。")
        season_df["month"] = season_df["month"].astype(str).str.zfill(2)
        season_df["time_id"] = (season_df["year"].astype(str) + season_df["month"]).astype(int)
        season_ref = season_df.drop_duplicates(subset=["year"])
        fact_df = fact_df.merge(season_ref[["season_id", "time_id", "year"]], on="year", how="left")

        # 5. Merge road_condition (use crash_id directly)
        print("merge road_condition_id...")
        fact_df["road_condition_id"] = fact_df["crash_id"]

        # 6. Merge vehicle_id (use crash_id directly)
        print("merge vehicle_id...")
        fact_df["vehicle_id"] = fact_df["crash_id"]

        # 7. Statistics driver_count (grouped by crash_id)
        print("statistics driver_count...")
        driver_counts = driver_df.groupby("crash_id")["driver_id"].count().reset_index()
        driver_counts.rename(columns={"driver_id": "driver_count"}, inplace=True)
        fact_df = fact_df.merge(driver_counts, on="crash_id", how="left")
        fact_df["driver_count"] = fact_df["driver_count"].fillna(0).astype(int)

        # 8. Merge location_id (with state)
        print("location_id...")
        fact_df = fact_df.merge(location_df[["location_id", "state"]], on="state", how="left")

        # 9. Merge population_id (by state + year)
        print(" population_id...")
        fact_df = fact_df.merge(population_df[["population_id", "state", "year"]], on=["state", "year"], how="left")

        # 10.Emulate lga_id (simplified with crash_id)
        print("lga_id...")
        fact_df["lga_id"] = fact_df["crash_id"]

        # 11.Annual crash & fatality summary
        print("计算年度统计...")
        yearly_stats = fact_df.groupby("year").agg({
            "crash_id": "count",
            "fatalities": "sum"
        }).reset_index().rename(columns={
            "crash_id": "yearly_crash_count",
            "fatalities": "yearly_fatality_count"
        })
        fact_df = fact_df.merge(yearly_stats, on="year", how="left")

        # 12. crash_date (January 1st of each year)
        print(" crash_date...")
        fact_df["crash_date"] = pd.to_datetime(fact_df["year"].astype(str) + "-01-01")

        # 13. Final field order
        columns_order = [
            "fact_id", "crash_id", "time_id", "location_id", "road_condition_id",
            "season_id", "vehicle_id", "driver_count", "population_id", "lga_id",
            "fatalities", "yearly_crash_count", "yearly_fatality_count", "crash_date"
        ]
        fact_df = fact_df[columns_order]

        # 14. Export to CSV
        print(" output/fact_table.csv ...")
        os.makedirs("output", exist_ok=True)
        fact_df.to_csv("output/fact_table.csv", index=False)

        print(f" fact_table.csv，共 {len(fact_df)} 条记录。")
        return True

    except Exception as e:
        print(f"error: {e}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    create_fact_table()
