from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Añade una columna FlightTime en formato UTC combinando la fecha y hora local de salida de cada vuelo,
    teniendo en cuenta la zona horaria del aeropuerto de origen.
    Se utiliza un fichero CSV con los timezones asociados a los códigos IATA para realizar la conversión
    mediante `F.to_utc_timestamp`.

    :param spark: Instancia activa de SparkSession.
    :param df: DataFrame de entrada con al menos las columnas ['Origin', 'FlightDate', 'DepTime'].
    :param fichero_timezones: CSV de los timezones de varios países.
    :return: DataFrame con una nueva columna 'FlightTime' en UTC, y sin columnas auxiliares ni metadata de timezone.
    """

    # Antes de empezar el ejercicio 2, debemos unir a los vuelos la zona horaria del aeropuerto de salida del vuelo,
    # utilizando el CSV de timezones.csv y uniéndolo por código IATA (columna Origin de los datos con columna iata_code
    # del CSV), dejando a null los timezones de los aeropuertos que no aparezcan en dicho fichero CSV si los hubiera.
    # Primero deberemos leer dicho CSV infiriendo el esquema e indicando que las columnas contienen encabezados.

    path_timezones = str(Path(__file__).parent) + "/resources/timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    df_with_tz = df.join(
        timezones_df,
        df["Origin"] == timezones_df["iata_code"],
        how="left"
    )


    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 2 (2 puntos)
    # ----------------------------------------

    # Añadir por la derecha una columna llamada FlightTime de tipo timestamp, a partir de las columnas
    # FlightDate y DepTime. Para ello:
    # (a) añade una columna llamada castedHour (que borraremos más adelante) como resultado de convertir la columna
    # DepTime a string, y aplicarle a la columna de string la función F.lpad para obtener una nueva columna en la
    # que se ha añadido el carácter "0" por la izquierda tantas veces como sea necesario. De ese modo nos
    # aseguramos de que tendrá siempre 4 caracteres.
    # (b) añade la columna FlightTime, de la forma "2023-12-25 20:04:00", concatenando lo siguiente (F.concat(...)):
    #    i. la columna resultante de convertir FlightDate a string. Esto nos dará la parte "2023-12-15"
    #    ii. un objeto columna constante, igual a " " (carácter espacio)
    #    iii. la columna resultante de tomar el substring que empieza en la posición 1 y tiene longitud 2. Revisa
    #         la documentación del método substr de la clase Column, y aplica (F.col(...).substr(...))
    #     iv. un objeto columna constante igual a ":"
    #     v. la columna resultante de tomar el substring que empieza en la posición 3 y tiene longitud 2. Los puntos
    #        iii, iv y v nos darán la parte "20:04:00" como string
    #     vi. Por último, aplica la función cast("timestamp") al objeto columna devuelto por concat:
    #         F.concat(...).cast("timestamp"). Los pasos i a v deben hacerse **en una única transformación**
    # (c) Finalmente, en una nueva transformación, reemplaza la columna FlightTime por el resultado de aplicar la
    #     función F.to_utc_timestamp("columna", "time zone") siendo "columna" la columna FlightTime y siendo
    #     "iana_tz" la columna que contiene la zona horaria en base a la cuál debe interpretarse el timestamp
    #     que ya teníamos en FlightTime
    # (d) Antes de devolver el DF resultante, borra las columnas que estaban en timezones_df, así como la columna
    #     castedHour

    # Paso (a): añade castedHour con F.lpad
    df_with_casted_hour = df_with_tz.withColumn(
        "castedHour",
        F.lpad(F.col("DepTime").cast("string"), 4, "0")
    )

    # Paso (b): construye FlightTime en local
    df_with_flight_time = df_with_casted_hour.withColumn(
        "FlightTime",
        F.concat(
            F.col("FlightDate").cast("string"),
            F.lit(" "),
            F.col("castedHour").substr(1, 2),
            F.lit(":"),
            F.col("castedHour").substr(3, 2),
            F.lit(":00")
        ).cast("timestamp")
    )

    # Paso (c): convierte a UTC con zona horaria
    df_with_flight_time_utc = df_with_flight_time.withColumn(
        "FlightTime",
        F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz"))
    )

    # Paso (d): elimina columnas auxiliares
    columnas_a_borrar = list(timezones_df.columns) + ["castedHour"]
    df_with_flight_time = df_with_flight_time_utc.drop(*columnas_a_borrar)
    
    return df_with_flight_time


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Añade información sobre el siguiente vuelo que parte del mismo aeropuerto (Origin), incluyendo la hora
    y aerolínea del siguiente vuelo, así como la diferencia en segundos respecto al vuelo actual.
    Utiliza una ventana particionada por 'Origin' y ordenada por 'FlightTime', junto con `lag` negativo para
    calcular esta información.

    :param df: DataFrame con al menos las columnas ['Origin', 'FlightTime', 'Reporting_Airline'].
    :return: DataFrame original con tres columnas adicionales: 'FlightTime_next', 'Airline_next' y 'diff_next'.
    """
    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 3 (2 puntos)
    # ----------------------------------------

    # Queremos pegarle a cada vuelo la información del vuelo que despega justo después de su **mismo
    # aeropuerto de origen**. En concreto queremos saber la hora de despegue del siguiente vuelo y la compañía aérea.
    # Para ello, primero crea una columna de pares (FlightTime, Reporting_Airline), y después crea otra columna
    # adicional utilizando la función F.lag(..., -1) con dicha columna, dentro de una ventana que
    # debe estar particionada adecuadamente y ordenada adecuadamente. No debes utilizar la transformación sort()
    # de los DF. Después, extrae los dos campos internos de la tupla como columnas llamadas "FlightTime_next" y "Airline_next",
    # y calcula una nueva columna diff_next con la diferencia en segundos entre la hora de salida de un vuelo y la
    # del siguiente, como la diferencia de ambas columnas (next menos actual) tras haberlas convertido al tipo "long".
    # El DF resultante de esta función debe ser idéntico al de entrada pero con 3 columnas nuevas añadidas por la
    # derecha, llamadas FlightTime_next, Airline_next y diff_next. Cualquier columna auxiliar debe borrarse.

    # Crea ventana particionada por aeropuerto y ordenada por hora de salida
    w = Window.partitionBy("Origin").orderBy("FlightTime")

    # Crea columna con tupla (FlightTime, Reporting_Airline)
    df_con_tupla = df.withColumn("tupla", F.struct("FlightTime", "Reporting_Airline"))

    # Aplica lag -1 para obtener la tupla del siguiente vuelo
    df_with_lag = df_con_tupla.withColumn("siguiente", F.lag("tupla", -1).over(w))

    # Extrae campos de la tupla
    df_split = df_with_lag \
        .withColumn("FlightTime_next", F.col("siguiente.FlightTime")) \
        .withColumn("Airline_next", F.col("siguiente.Reporting_Airline"))

    # Calcula diferencia en segundos entre ambos vuelos
    df_final = df_split.withColumn(
        "diff_next",
        (F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long"))
    )

    # Elimina columnas auxiliares
    df_with_next_flight = df_final.drop("tupla", "siguiente")

    return df_with_next_flight
