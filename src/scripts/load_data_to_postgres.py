#!/usr/bin/env python3
"""
Phase 2.2 - Load CSV data into PostgreSQL Database
This script reads the CSV files and writes them to PostgreSQL using PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

# Database configuration (must match your config.py)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "hotel_reservations"
DB_USER = "cs236_user"
DB_PASSWORD = "cs236_pass"

# JDBC URL for PostgreSQL
jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Path to PostgreSQL JDBC driver
jdbc_driver = "/Users/rushipatel/Desktop/cs236_project/src/database/postgresql-42.7.3.jar"

# Database connection properties
db_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    """Create Spark session with PostgreSQL JDBC driver"""
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("Load Hotel Reservations Data to PostgreSQL") \
        .config("spark.jars", jdbc_driver) \
        .config("spark.driver.extraClassPath", jdbc_driver) \
        .getOrCreate()
    
    print(f"Spark version: {spark.version}")
    return spark

def load_customer_reservations(spark):
    """Load customer-reservations.csv into PostgreSQL"""
    print("\n" + "="*60)
    print("Loading customer-reservations.csv...")
    print("="*60)
    
    df = spark.read.option("header", True).option("inferSchema", True).csv("data/customer-reservations.csv")
    
    row_count = df.count()
    print(f"Total rows: {row_count:,}")
    print("\nSchema:")
    df.printSchema()
    print("\nSample data:")
    df.show(5, truncate=False)
    
    # Write to PostgreSQL
    print(f"\nWriting to PostgreSQL table: customer_reservations...")
    df.write.jdbc(
        url=jdbc_url,
        table="customer_reservations",
        mode="overwrite",  # Use 'overwrite' to replace existing data, or 'append' to add to it
        properties=db_properties
    )
    print(f"✓ Successfully loaded {row_count:,} rows into customer_reservations table")
    
    return df

def load_hotel_booking(spark):
    """Load hotel-booking.csv into PostgreSQL"""
    print("\n" + "="*60)
    print("Loading hotel-booking.csv...")
    print("="*60)
    
    df = spark.read.option("header", True).option("inferSchema", True).csv("data/hotel-booking.csv")
    
    row_count = df.count()
    print(f"Total rows: {row_count:,}")
    print("\nSchema:")
    df.printSchema()
    print("\nSample data:")
    df.show(5, truncate=False)
    
    # Write to PostgreSQL
    print(f"\nWriting to PostgreSQL table: hotel_booking...")
    df.write.jdbc(
        url=jdbc_url,
        table="hotel_booking",
        mode="overwrite",
        properties=db_properties
    )
    print(f"✓ Successfully loaded {row_count:,} rows into hotel_booking table")
    
    return df

def load_merged_data(spark):
    """Load merged_data.csv into PostgreSQL"""
    print("\n" + "="*60)
    print("Loading merged_data.csv...")
    print("="*60)
    
    df = spark.read.option("header", True).option("inferSchema", True).csv("data/merged_data.csv")
    
    row_count = df.count()
    print(f"Total rows: {row_count:,}")
    print("\nSchema:")
    df.printSchema()
    print("\nSample data:")
    df.show(5, truncate=False)
    
    # Write to PostgreSQL
    print(f"\nWriting to PostgreSQL table: merged_dataset...")
    df.write.jdbc(
        url=jdbc_url,
        table="merged_dataset",
        mode="overwrite",
        properties=db_properties
    )
    print(f"✓ Successfully loaded {row_count:,} rows into merged_dataset table")
    
    return df

def verify_data_in_postgres(spark):
    """Verify that data was loaded successfully"""
    print("\n" + "="*60)
    print("VERIFICATION: Reading data back from PostgreSQL...")
    print("="*60)
    
    tables = ["customer_reservations", "hotel_booking", "merged_dataset"]
    
    for table in tables:
        print(f"\nTable: {table}")
        try:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table,
                properties=db_properties
            )
            count = df.count()
            print(f"  ✓ Rows in database: {count:,}")
            print(f"  Columns: {', '.join(df.columns)}")
        except Exception as e:
            print(f"  ✗ Error reading table: {e}")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Phase 2.2: Loading Hotel Reservations Data to PostgreSQL")
    print("=" * 60)
    
    # Check if JDBC driver exists
    if not os.path.exists(jdbc_driver):
        print(f"\n✗ ERROR: JDBC driver not found at: {jdbc_driver}")
        print("Please make sure postgresql-42.7.3.jar exists in the src/database directory")
        return
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Load each dataset
        load_customer_reservations(spark)
        load_hotel_booking(spark)
        load_merged_data(spark)
        
        # Verify data
        verify_data_in_postgres(spark)
        
        print("\n" + "=" * 60)
        print("✓ ALL DATA LOADED SUCCESSFULLY!")
        print("=" * 60)
        print("\nYou can now:")
        print("  1. Refresh your Flask web UI at http://localhost:5001")
        print("  2. Select a dataset from the dropdown")
        print("  3. Start querying and filtering data!")
        print("\n" + "=" * 60)
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

