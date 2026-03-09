package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("bigdata-meteo")
                .master("local[*]")
                .getOrCreate();

        try {
            String inputPath = "tmp/data/*.csv.gz";

            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("sep", ";")
                    .schema(DataSchema.csvSchema())
                    .csv(inputPath);

            System.out.println("STRUCTURE JEU DE DONNÉES");
            df.printSchema();

            System.out.println("ECHANTILLON 10 LIGNES");
            df.show(10, false);

            long rowCount = df.count();
            System.out.println("NOMBRE D'ENREGISTREMENTS");
            System.out.println("Nombre de ligne: " + rowCount);

        } finally {
            spark.stop();
        }
    }
}