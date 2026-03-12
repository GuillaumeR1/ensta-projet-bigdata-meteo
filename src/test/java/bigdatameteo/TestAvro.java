package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class TestAvro {

    @Test
    public void testAvro() {

    SparkSession spark = SparkSession.builder()
    .appName("test-avro")
    .master("local[*]")
    .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    try {

        String csvPath = "tmp/data/*.csv.gz";
        String avroPath = "output/avro";

        Dataset<Row> df = Main.runPipeline(spark, csvPath);

        System.out.println("\n***** Schéma des données formatées");
        df.printSchema();

        System.out.println("\n***** Exemple de données");
        df.show(10, false);

        DataConvertToAvro.writeAvro(df, avroPath);

        Dataset<Row> dfAvro = spark.read()
        .format("avro")
        .load(avroPath);

        System.out.println("\n***** Vérification lecture AVRO");
        dfAvro.show(10, false);
        System.out.println("\nNombre de lignes AVRO : " + dfAvro.count());

    } finally {
        spark.stop();
    }
    }
}