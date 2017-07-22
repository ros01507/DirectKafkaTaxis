Desarrollo para la recepción de datos de la web de Taxis de Nueva York a través de Spark Streaming,
desarrollo para la persistencia de esos datos "en bruto" en HDFS, desarrollo para la generación de
cubos OLAP.

La aplicación lee datos de topic de Kafka y a través de contexto spark-streaming persiste datos en bruto en HDFS, y por otro lado en
la misma ventana genera agreagaciones que persiste en cassandra.

Clase principal SaveHdfsCassandralase ,SparkSessionSingleton, y Statements.
