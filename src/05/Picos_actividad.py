import os
import sys
sys.path.insert(0, ".")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from commons.utils import FolderConfig

if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    spark = SparkSession.builder.appName(
        "Analizando la actividad dependiendo del tiempo"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    DIRECTORY = str(fc.input)


    # 1 - Imortación de datos

    df_oct = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "2019-Oct.csv"),
            sep=",",  # hay que indicarlo porque es .csv
            header=True,  # Si no le digo true coge la primera linea como dato.
            inferSchema=True
        )
    )

    df_jan = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "2020-Jan.csv"),
            sep=",",  # hay que indicarlo porque es .csv
            header=True,  # Si no le digo true coge la primera linea como dato.
            inferSchema=True
        )
    )

    # Pasamos la variable de tiempo a tipo 'timestamp'
    df_oct = df_oct.withColumn("event_time", F.to_timestamp("event_time"))
    df_jan = df_jan.withColumn("event_time", F.to_timestamp("event_time"))


    # OCTUBRE
    # Horas con mayor actividad de octubre

    activity_by_hour_oct = df_oct.groupBy(F.hour("event_time").alias("hour")).count()

    most_active_hours_oct = activity_by_hour_oct.orderBy(F.desc("count"))

    print('\n','Horas con mayor actividad de octubre','\n')
    most_active_hours_oct.show()


    # Eventos más repetidos de octubre

    most_repeated_events_oct = df_oct.groupBy("event_type").count()

    most_repeated_events_oct = most_repeated_events_oct.orderBy(F.desc("count"))

    print('Eventos más repetidos de octubre', '\n')
    most_repeated_events_oct.show()


    # Horas con mayor número de compras de octubre

    purchases_oct = df_oct.filter(F.col("event_type") == "purchase")

    purchases_by_hour_oct = purchases_oct.groupBy(F.hour("event_time").alias("hour")).count()

    most_purchase_hours_oct = purchases_by_hour_oct.orderBy(F.desc("count"))

    print('Horas con mayor número de compras de octubre', '\n')
    most_purchase_hours_oct.show()


    # Días de la semana con mayor actividad de octubre

    activity_by_day_of_week_oct = df_oct.groupBy(F.dayofweek("event_time").alias("day_of_week")).count()

    most_active_days_oct = activity_by_day_of_week_oct.orderBy(F.desc("count"))

    print('Días de la semana con mayor actividad de octubre', '\n')
    most_active_days_oct.show()


    # Días de la semana con mayores ventas de octubre

    purchases_by_weekday_oct = purchases_oct.groupBy(F.dayofweek("event_time").alias("weekday")).count()

    most_purchase_weekdays_oct = purchases_by_weekday_oct.orderBy(F.desc("count"))

    print('Días de la semana con mayores ventas de octubre', '\n')
    most_purchase_weekdays_oct.show()


    # ENERO
    # Horas con mayor actividad de enero

    activity_by_hour_jan = df_jan.groupBy(F.hour("event_time").alias("hour")).count()

    most_active_hours_jan = activity_by_hour_jan.orderBy(F.desc("count"))

    print('Horas con mayor actividad de enero', '\n')
    most_active_hours_jan.show()


    # Eventos más repetidos de enero

    most_repeated_events_jan = df_jan.groupBy("event_type").count()

    most_repeated_events_jan = most_repeated_events_jan.orderBy(F.desc("count"))

    print('Eventos más repetidos de enero', '\n')
    most_repeated_events_jan.show()


    # Horas con mayor número de compras de enero

    purchases_jan = df_jan.filter(F.col("event_type") == "purchase")

    purchases_by_hour_jan = purchases_jan.groupBy(F.hour("event_time").alias("hour")).count()

    most_purchase_hours_jan = purchases_by_hour_jan.orderBy(F.desc("count"))

    print('Horas con mayor número de compras de enero', '\n')
    most_purchase_hours_jan.show()


    # Días de la semana con mayor actividad de enero

    activity_by_day_of_week_jan = df_jan.groupBy(F.dayofweek("event_time").alias("day_of_week")).count()

    most_active_days_jan = activity_by_day_of_week_jan.orderBy(F.desc("count"))

    print('Días de la semana con mayor actividad de enero', '\n')
    most_active_days_jan.show()


    # Días de la semana con mayores ventas de enero

    purchases_by_weekday_jan = purchases_jan.groupBy(F.dayofweek("event_time").alias("weekday")).count()

    most_purchase_weekdays_jan = purchases_by_weekday_jan.orderBy(F.desc("count"))

    print('Días de la semana con mayores ventas de enero', '\n')
    most_purchase_weekdays_jan.show()

