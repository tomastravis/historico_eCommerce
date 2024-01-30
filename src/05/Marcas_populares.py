import os
import sys
sys.path.insert(0, ".")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from commons.utils import FolderConfig

if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    spark = SparkSession.builder.appName(
        "Analizando el comportamiento de los clientes de eCommerce"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1 - Reading all the relevant data sources (pipeline.extract())

    DIRECTORY = str(fc.input)

    df_oct = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "2019-Oct.csv"),
            sep=",",  # hay que indicarlo porque es .csv
            header=True,  # Si no le digo true coge la primera linea como dato.
            inferSchema=True
        )
    )

    # Marcas con los mayores ventas en octubre

    purchase_table_oct = df_oct.filter(F.col("event_type") == "purchase")

    purchase_with_brands_oct = purchase_table_oct.select(["brand","event_type","price"])

    brand_counts_oct = purchase_table_oct.groupBy("brand").agg(F.count("brand").alias("brand_count"))

    popular_brands_oct = brand_counts_oct.orderBy(F.desc("brand_count"))

    print('\nMarcas con los mayores número de ventas en octubre')
    popular_brands_oct.show()


    # Ingresos por marca en octubre

    brand_total_spent_oct = purchase_table_oct.groupBy("brand").agg(F.round(F.sum("price"), 2).alias("total_spent"))

    brands_by_spending_oct = brand_total_spent_oct.orderBy(F.desc("total_spent"))

    print('Ingresos por marca en octubre')
    brands_by_spending_oct.show()


    # Ahora para enero:
    df_jan = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "2020-Jan.csv"),
            sep=",",  # hay que indicarlo porque es .csv
            header=True,  # Si no le digo true coge la primera linea como dato.
            inferSchema=True
        )
    )


    # Marcas con mayores ventas en enero
    purchase_table_jan = df_jan.filter(F.col("event_type") == "purchase")

    purchase_with_brands_jan = purchase_table_jan.select(["brand", "event_type", "price"])

    brand_counts_jan = purchase_table_jan.groupBy("brand").agg(F.count("brand").alias("brand_count"))

    popular_brands_jan = brand_counts_jan.orderBy(F.desc("brand_count"))

    print('Marcas con mayores ventas en enero')
    popular_brands_jan.show()


    # Marcas con mayores ingresos en enero
    brand_total_spent_jan = purchase_table_jan.groupBy("brand").agg(F.round(F.sum("price"), 2).alias("total_spent"))

    brands_by_spending_jan = brand_total_spent_jan.orderBy(F.desc("total_spent"))

    print('Marcas con mayores ingresos en enero')
    brands_by_spending_jan.show()