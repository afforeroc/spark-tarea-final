import json
from datetime import timedelta
from loguru import logger

from pyspark.sql import SparkSession, functions as F
from pathlib import Path
from .motor_ingesta import MotorIngesta
from .agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from pyspark.sql.functions import col, lpad, concat_ws, to_timestamp, to_utc_timestamp

class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Inicializa la clase FlujoDiario leyendo la configuración para crear la SparkSession.

        :param config: Diccionario con la configuración general. Debe contener la clave 'EXECUTION_ENVIRONMENT' 
        con valor 'local' o 'databricks'.
        """
        with open(config_file, "r") as f:
            self.config = json.load(f)
        
        env = self.config.get("EXECUTION_ENVIRONMENT")
        self.spark = SparkSession.getActiveSession()
        if not(self.spark):
            if env == "local":
                self.spark = SparkSession.builder \
                    .appName("Flujo Diario Local") \
                    .getOrCreate()
            elif env == "databricks":
                try:
                    import databricks.connect
                    self.spark = databricks.connect.get()
                except ImportError:
                    raise ImportError("Debes tener instalado databricks-connect para usar este entorno.")
            else:
                raise ValueError(f"Entorno '{env}' no reconocido. Usa 'local' o 'databricks'.")


    def procesa_diario(self, data_file: str):
        """
        Procesa el archivo diario de vuelos, añadiendo columnas adicionales y guardando el resultado en una tabla gestionada
        particionada por 'FlightDate'.

        :param data_file: Ruta del archivo de entrada (archivo JSON de vuelos)
        :return: None
        """
        try:
            # Paso 1: Crear motor de ingesta y cargar el DataFrame desde el archivo
            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file)

            # Cachear el dataframe para evitar lecturas repetidas
            flights_df = flights_df.cache()

            # Paso 2: Añadir columna FlightTime en UTC
            flights_with_utc = aniade_hora_utc(self.spark, flights_df)

            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            # Paso 3: Leer el día previo si existe, para resolver el intervalo faltante entre días
            dia_actual = flights_with_utc.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)

            try:
                # Intentamos leer la partición del día previo desde la tabla de destino
                flights_previo = self.spark.read.table(self.config["output_table"]).where(
                    F.col("FlightDate") == F.lit(dia_previo)
                )
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            # Si se encuentra datos previos, los unimos con los datos actuales
            if flights_previo:
                # Añadir columnas con valor None y hacer cast para que coincidan los tipos
                cols_to_add = [col for col in flights_with_utc.columns if col not in flights_previo.columns]
                for col in cols_to_add:
                    flights_previo = flights_previo.withColumn(col, F.lit(None).cast(flights_with_utc.schema[col].dataType))
                
                # Alinear el orden de las columnas antes de la unión
                flights_previo = flights_previo.select(flights_with_utc.columns)

                # Unir los datos previos con los actuales
                df_unido = flights_previo.unionByName(flights_with_utc)

                # Guardar el resultado temporalmente en una tabla provisional
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")
            else:
                # Si no hay datos previos, usamos los datos actuales
                df_unido = flights_with_utc

            # Paso 4: Añadir información del siguiente vuelo
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            # Paso 5: Escribir el DataFrame resultante en la tabla externa particionada por 'FlightDate'
            df_with_next_flight \
                .coalesce(self.config["output_partitions"]) \
                .write \
                .mode("overwrite") \
                .option("path", self.config.get("output_path", "default")) \
                .option("partitionOverwriteMode", "dynamic") \
                .partitionBy("FlightDate") \
                .saveAsTable(self.config["output_table"])

            # Borrar la tabla provisional si fue creada
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo procesar el fichero {data_file}. Error: {str(e)}")
            raise e





if __name__ == '__main__':
    
    spark = SparkSession.builder.getOrCreate()   # sólo si lo ejecutas localmente
    path_json_segundo_dia = str(Path(__file__).parent.parent / "data" / "2023-01-02.json")

    path_config_flujo_diario = str(Path(__file__).parent.parent / "config" / "config.json")      # ruta del fichero config.json, que no pertenece al paquete
    path_json_primer_dia = str(Path(__file__).parent.parent / "data" / "2023-01-01.json")           # ruta del fichero JSON de un día concreto que queremos ingestar, en nuestro caso 2023-01-01.json


    # 1. Inicializamos FlujoDiario para que cargue config y SparkSession
    flujo_diario = FlujoDiario(path_config_flujo_diario)

    
    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel
    
