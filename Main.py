from etl_openFoodFacts import init_spark, read_data, clean_data, create_dataframes, generate_menu, write_to_postgres
from pyspark.sql.functions import col
from peuplement import Peuplement

if __name__ == "__main__":
    spark = init_spark()
    file_path = "input/"
    fichier = file_path + "data_openfood.csv"

    df = read_data(spark, file_path)
    df_cleaned = clean_data(df)

    # Créer les DataFrames pour les régimes
    df_regimes = create_dataframes(spark)

    # Charger les utilisateurs en base de données
    peuplement = Peuplement(spark)
    df_users = peuplement.create_users_dataframe()
    peuplement.write_to_postgres(df_users, "users")

    # Afficher les utilisateurs chargés
    print("Utilisateurs chargés en base de données :")
    df_users.show()

    # Récupérer les utilisateurs depuis la base de données
    df_users = peuplement.read_from_postgres("users")

    # Afficher les utilisateurs récupérés
    print("Utilisateurs récupérés depuis la base de données :")
    df_users.show()

    # Générer le menu pour un utilisateur spécifique
    user_id = 5
    user_details_list = df_users.filter(col("User_ID") == user_id).collect()

    if user_details_list:
        user_details = user_details_list[0]
        menu = generate_menu(user_details, df_cleaned, df_regimes)
        menu.show(truncate=False)
    else:
        print(f"Utilisateur avec l'ID {user_id} n'a pas été trouvé.")

    # Écrire les données nettoyées et les régimes en base de données
    write_to_postgres(df_cleaned, "cleaned_data")
    write_to_postgres(df_regimes, "regimes")
