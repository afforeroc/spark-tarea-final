import json
from datetime import timedelta
from loguru import logger

from pyspark.sql import SparkSession, functions as F
from agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto

### Importaciones adicionales
from motor_ingesta import MotorIngesta
from agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
#############################

class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Completa la documentación
        :param config_file:
        """
        # Leer como diccionario el fichero json indicado en la ruta config_file, usando json.load(f) del paquete json
        # y almacenarlo en self.config. Además, crear la SparkSession si no existiese usando
        # SparkSession.builder.getOrCreate() que devolverá la sesión existente, o creará una nueva si no existe ninguna

        self.spark = SparkSession.builder.getOrCreate()     # sustituye None por lo adecuado para recuperar la SparkSession existente o crear una   
        with open(config_file, "r") as file:                # sustituye None por lo adecuado para leer el fichero de config como diccionario
            self.config = json.load(file)


    def procesa_diario(self, data_file: str):
        """
        Completa la documentación
        :param data_file:
        :return:
        """

        try:
            # Procesamiento diario: crea un nuevo objeto motor de ingesta con self.config, invoca a ingesta_fichero,
            # después a las funciones que añaden columnas adicionales, y finalmente guarda el DF en la tabla indicada en
            # self.config["output_table"], que debe crearse como tabla manejada (gestionada), sin usar ningún path,
            # siempre particionando por FlightDate. Tendrás que usar .write.option("path", ...).saveAsTable(...) para
            # indicar que queremos crear una tabla externa en el momento de guardar.
            # Conviene cachear el DF flights_df así como utilizar el número de particiones indicado en
            # config["output_partitions"]

            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file)
            flights_df.cache()

            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc = aniade_hora_utc(self.spark, flights_df)  # reemplaza por la llamada adecuada
   
            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            # Paso 2. Para resolver el ejercicio 4 que arregla el intervalo faltante entre días,
            # hay que leer de la tabla self.config["output_table"] la partición del día previo si existiera. Podemos
            # obviar este código hasta llegar al ejercicio 4 del notebook
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"])\
                    .where(F.col("FlightDate") == F.lit(dia_previo))
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:
                # añadir columnas a F.lit(None) haciendo cast al tipo adecuado de cada una, y unirlo con flights_previo.
                # OJO: hacer select(flights_previo.columns) para tenerlas en el mismo orden antes de
                # la unión, ya que la columna de partición se había ido al final al escribir
                columnas_faltantes = set(flights_with_utc.columns) - set(flights_previo.columns)
                for col in columnas_faltantes:
                    tipo = flights_with_utc.schema[col].dataType
                    flights_previo = flights_previo.withColumn(col, F.lit(None).cast(tipo))
                flights_previo = flights_previo.select(flights_with_utc.columns)
                df_unido = flights_previo.unionByName(flights_with_utc)
                # Spark no permite escribir en la misma tabla de la que estamos leyendo. Por eso salvamos
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc           # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            # Paso 4. Escribimos el DF en la tabla externa config["output_table"] con ubicación config["output_path"], con
            # el número de particiones indicado en config["output_partitions"]
            # df_with_next_flight.....(...)..write.mode("overwrite").option("partitionOverwriteMode", "dynamic")....
            df_with_next_flight\
                .coalesce(self.config["output_partitions"])\
                .write\
                .mode("overwrite")\
                .option("partitionOverwriteMode", "dynamic")\
                .option("path", self.config.get("output_path", "xxx"))\
                .partitionBy("FlightDate")\
                .saveAsTable(self.config["output_table"])


            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()   # sólo si lo ejecutas localmente
    path_config_flujo_diario = "../config/config.json"
    path_json_primer_dia = "../data/flights_json/landing/2023-01-01.json"
    flujo = FlujoDiario(path_config_flujo_diario)
    flujo.procesa_diario(path_json_primer_dia)

    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel