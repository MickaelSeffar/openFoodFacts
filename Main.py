from etl_openFoodFacts import init_spark, read_data, clean_data, create_dataframes, generate_menu, write_to_postgres
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = init_spark()
    file_path = "input/"
    fichier = file_path + "data_openfood.csv"

    df = read_data(spark, file_path)
    df_cleaned = clean_data(df)

    df_regimes, df_users = create_dataframes(spark)

    user_id = 5
    user_details = df_users.filter(col("User_ID") == user_id).collect()[0]
    menu = generate_menu(user_details, df_cleaned, df_regimes)
    menu.show(truncate=False)

    write_to_postgres(df_cleaned, "cleaned_data")
    write_to_postgres(df_regimes, "regimes")
    write_to_postgres(df_users, "users")