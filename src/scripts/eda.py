from pyspark.sql import SparkSession, functions as F

# Initialize Spark
spark = SparkSession.builder.appName("Phase1-EDA").getOrCreate()

# Load datasets
reservations = spark.read.option("header", True).csv(
    "data/raw/customer-reservations.csv", inferSchema=True
)
bookings = spark.read.option("header", True).csv(
    "data/raw/hotel-booking.csv", inferSchema=True
)


def basic_info(df, name):
    print(f"\n=== {name} ===")
    print("Row count:", df.count())
    df.printSchema()


def null_counts(df, name):
    print(f"\n--- Null Value Counts: {name} ---")
    df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).show(truncate=False)


def distinct_values(df, cols, name):
    print(f"\n--- Distinct Values: {name} ---")
    for c in cols:
        if c in df.columns:
            print(f"\nColumn: {c}")
            df.groupBy(c).count().orderBy("count", ascending=False).show(
                10, truncate=False
            )


def numeric_summary(df, cols, name):
    print(f"\n--- Numeric Summary: {name} ---")
    for c in cols:
        if c in df.columns:
            print(f"\nColumn: {c}")
            df.describe([c]).show()


# 1. Basic info
basic_info(reservations, "Customer Reservations")
basic_info(bookings, "Hotel Bookings")

# 2. Null counts
null_counts(reservations, "Customer Reservations")
null_counts(bookings, "Hotel Bookings")

# 3. Distinct values (categorical checks)
distinct_values(reservations, ["booking_status", "market_segment_type"], "Reservations")
distinct_values(bookings, ["booking_status", "market_segment"], "Bookings")

# 4. Numeric summaries
numeric_summary(reservations, ["lead_time", "avg_price_per_room"], "Reservations")
numeric_summary(bookings, ["lead_time", "avg_price_per_room"], "Bookings")

# Stop Spark
spark.stop()
