import os
import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, sum
from commons.utils import FolderConfig


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    spark = SparkSession.builder.appName("TopSellingProduct").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    DIRECTORY = str(fc.input)

    # Definir el esquema de los datos
    schema = ("event_time TIMESTAMP, "
              "event_type STRING, "
              "product_id LONG, "
              "category_id LONG, "
              "category_code STRING, "
              "brand STRING, "
              "price DOUBLE, "
              "user_id LONG, "
              "user_session STRING")

    # Rutas completas de los archivos
    ruta_octubre = os.path.join(DIRECTORY, "2019-Oct.csv")
    ruta_enero = os.path.join(DIRECTORY, "2020-Jan.csv")

    # Cargar los datos de octubre y enero
    logs_oct = (
        spark.read.csv(
            path=ruta_octubre,
            sep=",",
            header=True,
            inferSchema=False,
            schema=schema
        )
    )

    logs_enero = (
        spark.read.csv(
            path=ruta_enero,
            sep=",",
            header=True,
            inferSchema=False,
            schema=schema
        )
    )

    # Guardar los resultados de octubre
    most_sold_october = logs_oct.groupBy("product_id", "brand").agg(
        count("*").alias("sales_count_october"),
        sum("price").alias("total_sales_price_october")
    ).orderBy("sales_count_october", ascending=False).limit(1)

    most_sold_october.coalesce(1).write.csv(
        os.path.join(fc.output, "most_sold_october.csv"),
        header=True,
        mode="overwrite"
    )

    # Guardar los resultados de enero
    most_sold_january = logs_enero.groupBy("product_id", "brand").agg(
        count("*").alias("sales_count_january"),
        sum("price").alias("total_sales_price_january")
    ).orderBy("sales_count_january", ascending=False).limit(1)

    most_sold_january.coalesce(1).write.csv(
        os.path.join(fc.output, "most_sold_january.csv"),
        header=True,
        mode="overwrite"
    )

    # Mostrar el producto más vendido de octubre
    print('\n', "Producto más vendido de octubre:")
    most_sold_october_data = most_sold_october.first()
    print("ID de Producto:", most_sold_october_data["product_id"])
    print("Nombre del Producto (Marca):", most_sold_october_data["brand"])
    print("Cantidad de Ventas en octubre:", most_sold_october_data["sales_count_october"])
    print("Suma del Precio de Ventas en octubre:", most_sold_october_data["total_sales_price_october"])

    # Obtener el ID del producto más vendido de octubre
    most_sold_october_data = most_sold_october.first()
    most_sold_product_id = most_sold_october_data["product_id"]

    # Filtrar eventos relacionados con el producto más vendido de octubre
    most_sold_october_events = logs_oct.filter(col("product_id") == most_sold_product_id)

    # Mostrar los eventos del producto más vendido de octubre
    print("\nEventos del Producto Más Vendido de Octubre:")
    most_sold_october_events.show(truncate=False)

    # Encontrar el usuario que realizó las mayores compras del producto más vendido
    user_with_highest_sales = most_sold_october_events.groupBy("user_id").agg(
        sum("price").alias("total_sales")
    ).orderBy("total_sales", ascending=False).limit(1).first()

    # Mostrar información sobre el usuario que realizó las mayores compras
    print("\nUsuario con las Mayores Compras del Producto Más Vendido de Octubre:")
    print("User ID:", user_with_highest_sales["user_id"])
    print("Total de Compras:", user_with_highest_sales["total_sales"])

    # Mostrar el producto más vendido de enero
    print("\nProducto más vendido de enero:")
    most_sold_january_data = most_sold_january.first()
    print("ID de Producto:", most_sold_january_data["product_id"])
    print("Nombre del Producto (Marca):", most_sold_january_data["brand"])
    print("Cantidad de Ventas en enero:", most_sold_january_data["sales_count_january"])
    print("Suma del Precio de Ventas en enero:", most_sold_january_data["total_sales_price_january"])

    # Obtener el ID del producto más vendido de enero
    most_sold_january_data = most_sold_january.first()
    most_sold_product_id_january = most_sold_january_data["product_id"]

    # Filtrar eventos relacionados con el producto más vendido de enero
    most_sold_january_events = logs_enero.filter(col("product_id") == most_sold_product_id_january)

    # Mostrar los eventos del producto más vendido de enero
    print("\nEventos del Producto Más Vendido de Enero:")
    most_sold_january_events.show(truncate=False)

    # Encontrar el usuario que realizó las mayores compras del producto más vendido en enero
    user_with_highest_sales_january = most_sold_january_events.groupBy("user_id").agg(
        sum("price").alias("total_sales")
    ).orderBy("total_sales", ascending=False).limit(1).first()

    # Mostrar información sobre el usuario que realizó las mayores compras en enero
    print("\nUsuario con las Mayores Compras del Producto Más Vendido de Enero:")
    print("User ID:", user_with_highest_sales_january["user_id"])
    print("Total de Compras:", user_with_highest_sales_january["total_sales"])

    # Cerrar la sesión de Spark
    spark.stop()




