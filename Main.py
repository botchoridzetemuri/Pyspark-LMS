# 1. IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 2. SETUP THE ENGINE
spark = SparkSession.builder \
    .appName("Pagila_StepByStep") \
    .config("spark.jars", "/Users/temuribotchoridze/Downloads/postgresql-42.7.11.jar") \
    .getOrCreate()

# 3. SETUP THE DATABASE CONNECTION
def load_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5435/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

# 4. GRAB ALL INGREDIENTS (Fast because PySpark is lazy!)
print("Loading all database blueprints...")
df_film = load_table("film")
df_category = load_table("category")
df_film_category = load_table("film_category")
df_actor = load_table("actor")
df_film_actor = load_table("film_actor")
df_inventory = load_table("inventory")
df_rental = load_table("rental")
df_payment = load_table("payment")
df_customer = load_table("customer")
df_address = load_table("address")
df_city = load_table("city")


print(" Task 1: Number of movies in each category ")
task1_df = df_category.join(df_film_category, "category_id") \
    .groupBy("name") \
    .count() \
    .orderBy(F.col("count").desc())

task1_df.show()

print(" Task 2: Top 10 actors by rentals ")
task2_df = df_rental.join(df_inventory, "inventory_id") \
        .join(df_film_actor,"film_id") \
        .join(df_actor,"actor_id") \
        .groupBy("actor_id","first_name","last_name") \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10)

task2_df.show()

print(" Task 3: Category with most money spent")

task3_df = df_payment.join(df_rental, "rental_id") \
    .join(df_inventory, "inventory_id") \
    .join(df_film_category, "film_id") \
    .join(df_category, "category_id") \
    .groupBy("name") \
    .agg(F.sum("amount").alias("total_spent")) \
    .orderBy(F.col("total_spent").desc()) \
    .limit(1)

task3_df.show()


print(" Task 4: Movies not in inventory ")

task4_df = df_film.join(df_inventory, "film_id", "left_anti") \
    .select("title")

task4_df.show()


print("Task 5: Top 3 actors in 'Children' category")

children_actors = df_category.filter(F.col("name") == "Children") \
    .join(df_film_category, "category_id") \
    .join(df_film_actor, "film_id") \
    .join(df_actor, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(F.count("film_id").alias("movie_count"))

window_spec = Window.orderBy(F.col("movie_count").desc())

task5_df = children_actors.withColumn("rank", F.dense_rank().over(window_spec)) \
    .filter(F.col("rank") <= 3) \
    .drop("rank") 

task5_df.show()



print(" Task 6: Active vs Inactive customers by city ")

task6_df = df_customer.join(df_address, "address_id") \
    .join(df_city, "city_id") \
    .groupBy("city") \
    .agg(
        F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_customers"),
        F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_customers")
    ) \
    .orderBy(F.col("inactive_customers").desc())

task6_df.show()






print("Task 7: Highest rental hours by city naming rules")

base_df = df_rental.withColumn(
        "rental_hours",
        (F.unix_timestamp("return_date") - F.unix_timestamp("rental_date")) / 3600
    ).join(df_inventory, "inventory_id") \
     .join(df_film_category, "film_id") \
     .join(df_category, "category_id") \
     .join(df_customer, "customer_id") \
     .join(df_address, "address_id") \
     .join(df_city, "city_id")

print("Highest rental hours for cities starting with 'a':")
base_df.filter(F.lower(F.col("city")).startswith("a")) \
    .groupBy("name") \
    .agg(F.sum("rental_hours").alias("total_hours")) \
    .orderBy(F.col("total_hours").desc()) \
    .limit(1) \
    .show()

print("Highest rental hours for cities containing '-':")
base_df.filter(F.col("city").contains("-")) \
    .groupBy("name") \
    .agg(F.sum("rental_hours").alias("total_hours")) \
    .orderBy(F.col("total_hours").desc()) \
    .limit(1) \
    .show()