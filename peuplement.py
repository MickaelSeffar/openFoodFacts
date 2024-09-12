from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class Peuplement:
    def __init__(self, spark):
        self.spark = spark

    def create_users_dataframe(self):
        schema_users = StructType([
            StructField("User_ID", IntegerType(), True),
            StructField("Nom", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Sexe", StringType(), True),
            StructField("Regime_ID", IntegerType(), True),
            StructField("Email", StringType(), True)
        ])

        data_users = [
            (1, "Alice", 30, "F", 1, "alice@example.com"),
            (2, "Bob", 45, "M", 2, "bob@example.com"),
            (3, "Charlie", 29, "M", 3, "charlie@example.com"),
            (4, "Diana", 34, "F", 4, "diana@example.com"),
            (5, "Eve", 40, "F", 5, "eve@example.com"),
            (6, "Frank", 50, "M", 6, "frank@example.com"),
            (7, "Grace", 27, "F", 7, "grace@example.com"),
            (8, "Henry", 38, "M", 8, "henry@example.com")
        ]

        df_users = self.spark.createDataFrame(data_users, schema_users)
        return df_users

    def write_to_postgres(self, df, table_name, mode="overwrite"):
        url = "jdbc:postgresql://localhost:5432/openfoodfacts"
        properties = {
            "user": "",
            "password": "",
            "driver": "org.postgresql.Driver"
        }
        print(f"Writing to PostgreSQL table {table_name}")
        df.show()  # Afficher les données avant de les écrire
        df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

    def read_from_postgres(self, table_name):
        url = "jdbc:postgresql://localhost:5432/openfoodfacts"
        properties = {
            "user": "",
            "password": "",
            "driver": "org.postgresql.Driver"
        }
        df = self.spark.read.jdbc(url=url, table=table_name, properties=properties)
        return df
