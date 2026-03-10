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

            String inputPath = "tmp/data/*.csv.gz";
            String outputPath = "output/avro";

            // Lecture CSV
            Dataset<Row> dfBrut = spark.read()
                    .option("header", "true")
                    .option("sep", ";")
                    .schema(DataSchema.csvSchema())
                    .csv(inputPath);

            // Formatage
            Dataset<Row> dfFormate = DataFormatage.formater(dfBrut);

            DataConvertToAvro.writeAvro(dfFormate, outputPath);

            Dataset<Row> dfAvro = spark.read()
                    .format("avro")
                    .load(outputPath);

            System.out.println("\n***** Schéma AVRO :");
            dfAvro.printSchema();

            System.out.println("\n***** Aperçu donnée AVRO :");
            dfAvro.show(10, false);

            long count = dfAvro.count();

            System.out.println("Nombre total de lignes : " + count);
        }

        finally {
            spark.stop();
        }
    }
}