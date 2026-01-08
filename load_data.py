import pandas as pd
from sqlalchemy import create_engine

# Your database URL
DATABASE_URL = "postgresql://hospital_capacity_database_user:fc7fWTgvmHGQvZaMpPD3ueTcPzE9KbDV@dpg-d5g2ft4hg0os73816ol0-a.virginia-postgres.render.com/hospital_capacity_database"
engine = create_engine(DATABASE_URL)

DATASETS = {
    "elective_surgeries": r"C:/Users/HP/Desktop/SpireHealthApplication/Data_API/hospital_data/elective_surgeries.csv",
    "staffing": r"C:/Users/HP/Desktop/SpireHealthApplication/Data_API/hospital_data/staffing.csv",
    "admissions": r"C:/Users/HP/Desktop/SpireHealthApplication/Data_API/hospital_data/admissions.csv",
    "bed_inventory": r"C:/Users/HP/Desktop/SpireHealthApplication/Data_API/hospital_data/bed_inventory.csv",
    "ed_arrivals": r"C:/Users/HP/Desktop/SpireHealthApplication/Data_API/hospital_data/ed_arrivals.csv"
}

def load_data():
    print("Loading hospital data to PostgreSQL...")
    
    for table_name, file_path in DATASETS.items():
        print(f"Loading {table_name}...")
        
        try:
            df = pd.read_csv(file_path)
            print(f"  Read {len(df):,} rows")
            
            df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=10000)
            print(f"Loaded to PostgreSQL")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print("\nAll data loaded!")
    print("Run the API with: python api/index.py")

if __name__ == "__main__":
    load_data()