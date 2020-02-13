- DESARROLLO DE CUBOS OLAP CON APACHE KAFKA Y SPARK STREAMING

**Descripción del trabajo:

Uno de los procesos más costosos en los entornos de Big Data y de los entornos de analítica en general, es tener que realizar operaciones   "full-scan" para la obtención de métricas y el análisis de datos. Es por ello que la generación de cubos OLAP, cuando es posible, para   la agrupación de datos en tiempo real, y antes de ser persistidos, es una de las técnicas más usadas en entornos con data lakes.   
En este proyecto se ha creado una aplicación con Spark Streaming que recibe datos a través de Apache Kafka y en base a una configuración,   genera 2 cubos OLAP y los persiste en Apache Cassandra. Por otro lado, los datos "en bruto", son persistidos en HDFS con formato Apache   Parquet.

**Objetivo**: Aceleración de consultas a través del cálculo al vuelo de datos en streaming antes de ser persistidos.  

**Tecnologías usadas**: Apache Spark, Apache Kafka,Apache Hadoop(HDFS), Apache Parquet, Apache Cassandra, Scala.  

**Origen de los datos**: Simulador basado en datos reales de taxis de Nueva York.

**Plan seguido**:

- Desarrollo de generador de datos enviados datos a un topic de Kafka.
- Lectura datos csv de topic de Kafka DirectStream con spark-streaming.
- Desarrollo para persistencia de datos "en bruto" en HDFS.
- Desarrollo para la generación de cubos OLAP, y pesistencia en Cassandra.

**Clases**: SaveHdfsCassandra ,SparkSessionSingleton, y Statements.


