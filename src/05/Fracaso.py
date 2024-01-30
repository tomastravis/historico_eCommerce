#Para ejecutar el código: spark-submit src/Fracaso.py data/2019-Oct.csv data/2020-Jan.csv

import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import when

# Definición de la función para calcular la tasa de fracaso
def calculate_failure_rate(df):
    add_to_cart_df = df.filter(F.col("event_type") == "cart").select("product_id")
    remove_from_cart_df = df.filter(F.col("event_type") == "remove_from_cart").select("product_id")
    add_counts = add_to_cart_df.groupBy("product_id").count().withColumnRenamed("count", "add_count")
    remove_counts = remove_from_cart_df.groupBy("product_id").count().withColumnRenamed("count", "remove_count")
    combined_df = add_counts.join(remove_counts, "product_id", "outer").na.fill(0)
    failure_rate_df = combined_df.withColumn("failure_rate",
                                             when(F.col("add_count") >= F.col("remove_count"),
                                                  F.col("remove_count") / F.col("add_count")).otherwise(None))
    return failure_rate_df.filter(F.col("failure_rate") == 1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Fracaso.py <2019-Oct.csv> <2020-Jan.csv>", file=sys.stderr)
        sys.exit(-1)

    file1, file2 = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("Ecommerce Failure Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df1 = spark.read.csv(f'{file1}', header=True, inferSchema=True)
    df2 = spark.read.csv(f'{file2}', header=True, inferSchema=True)

    top_failure_products_oct = calculate_failure_rate(df1)
    top_failure_products_jan = calculate_failure_rate(df2)

    top_failure_products_oct.show()
    top_failure_products_jan.show()

    count_oct_1 = top_failure_products_oct.count()
    count_jan_1 = top_failure_products_jan.count()

    coincident_products_1 = top_failure_products_oct.join(top_failure_products_jan, "product_id").select("product_id")
    count_coincident_1 = coincident_products_1.count()

    coincident_products_1.show()

    # Creación de un DataFrame con los conteos para mostrar en forma tabular
    summary_df = spark.createDataFrame([
        Row(Mes="Octubre", Productos_Con_Tasa_De_Fracaso_1=count_oct_1),
        Row(Mes="Enero", Productos_Con_Tasa_De_Fracaso_1=count_jan_1),
        Row(Mes="Coincidentes en ambos meses", Productos_Con_Tasa_De_Fracaso_1=count_coincident_1)
    ])

    summary_df.show()


