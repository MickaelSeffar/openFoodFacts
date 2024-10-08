from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, rand
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random

def init_spark():
    file_path = "lib/"
    return SparkSession.builder \
        .appName("DataWithoutImages") \
        .config("spark.jars", file_path + "postgresql-42.7.4.jar") \
        .getOrCreate()

def read_data(spark, file_path):
    return spark.read.csv(file_path, sep='\t', header=True)

def clean_data(df):
    colonnes_inutiles = [
        'code', 'created', 'last', 'url', 'image', 'creator', 'categories', 'abbreviated_product_name',
        'generic_name', 'packaging', 'brands', 'tags', 'origins', 'cities', 'countries', 'allergens',
        'traces', 'additives', 'states', 'brand_owner', 'owner', 'main_category', 'food_groups',
        'unique_scans_n', 'labels', '_t', '_datetime ', 'purchase_places', 'stores'
    ]

    colonnes_critiques = [
        "energy_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g",
        "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g"
    ]

    df_cleaned = df.drop(*[col for col in df.columns if any(mot in col for mot in colonnes_inutiles)])
    df_cleaned = df_cleaned.filter(col("product_name").isNotNull())

    for col_name in colonnes_critiques:
        df_cleaned = df_cleaned.filter((col(col_name).isNotNull()) & (~isnan(col(col_name))))

    df_cleaned = df_cleaned.filter(
        (col("energy_100g") > 0) & (col("energy_100g") < 1000) &
        (col("fat_100g") >= 0) & (col("fat_100g") < 150) &
        (col("saturated-fat_100g") >= 0) & (col("saturated-fat_100g") < 150) &
        (col("carbohydrates_100g") >= 0) & (col("carbohydrates_100g") < 150) &
        (col("sugars_100g") >= 0) & (col("sugars_100g") < 150) &
        (col("fiber_100g") >= 0) & (col("fiber_100g") < 150) &
        (col("proteins_100g") >= 0) & (col("proteins_100g") < 150) &
        (col("salt_100g") >= 0) & (col("salt_100g") < 150) &
        (col("sodium_100g") >= 0) & (col("sodium_100g") < 5000)
    )

    return df_cleaned

def create_dataframes(spark):
    schema_regimes = StructType([
        StructField("Regime_ID", IntegerType(), True),
        StructField("Nom", StringType(), True),
        StructField("Calories_max", IntegerType(), True),
        StructField("Glucides_max", IntegerType(), True),
        StructField("Lipides_max", IntegerType(), True),
        StructField("Proteines_min", IntegerType(), True),
        StructField("Sodium_max", IntegerType(), True)
    ])

    data_regimes = [
        (1, "Cétogène", 2000, 20, 150, 70, 2300),
        (2, "DASH", 2100, 250, 60, 80, 1500),
        (3, "Méditerranéen", 2500, 300, 70, 80, 2300),
        (4, "Végétarien", 2200, 250, 60, 70, 2000),
        (5, "Végétalien", 2000, 250, 50, 65, 1800),
        (6, "Paleo", 2300, 150, 100, 100, 2000),
        (7, "Index Glycémique Bas", 2000, 180, 70, 90, 2000),
        (8, "Régime de la Plaque", 2200, 200, 60, 100, 1800)
    ]

    df_regimes = spark.createDataFrame(data_regimes, schema_regimes)

    return df_regimes

def generate_menu(user_data, clean_data, regimes):
    user_regime = regimes.filter(col("Regime_ID") == user_data["Regime_ID"]).collect()[0]

    filtered_data = clean_data.filter(
        (col("energy_100g") <= user_regime["Calories_max"]) &
        (col("carbohydrates_100g") <= user_regime["Glucides_max"]) &
        (col("fat_100g") <= user_regime["Lipides_max"]) &
        (col("proteins_100g") >= user_regime["Proteines_min"]) &
        (col("sodium_100g") <= user_regime["Sodium_max"])
    )

    menu = filtered_data.orderBy(rand(random.randint(0, 100000))).limit(21)
    return menu

def write_to_postgres(df, table_name, mode="overwrite"):
    url = "jdbc:postgresql://localhost:5432/openfoodfacts"
    properties = {
        "user": "",
        "password": "",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
