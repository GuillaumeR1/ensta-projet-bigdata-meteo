package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class TestDataFormat {

    @Test
    public void testFormatage() {

        SparkSession spark = SparkSession.builder()
                .appName("test-data-formatage")
                .master("local[*]")
                .getOrCreate();


        spark.sparkContext().setLogLevel("ERROR");

        try {
            String inputPath = "tmp/data/*.csv.gz";

            Dataset<Row> dfInspect = spark.read()
                    .option("header", "true")
                    .option("sep", ";")
                    .csv("tmp/data/*.csv.gz");

            System.out.println("===== NOMS RÉELS DES COLONNES =====");
            for (String c : dfInspect.columns()) {
                System.out.println(c);
            }

            // Données brutes
            Dataset<Row> dfBrut = spark.read()
                    .option("header", "true")
                    .option("sep", ";")
                    .schema(DataSchema.csvSchema())
                    .csv(inputPath);

            // Données formatées
            Dataset<Row> dfFormate = DataFormatage.formater(dfBrut);

            System.out.println("\n***** Schéma du jeu de donnée");
            dfFormate.printSchema();

            System.out.println("\n***** 10 première lignes des données brutes :");
            dfBrut.show(10, false);

            System.out.println("\n***** 10 première lignes des données formatées :");
            dfFormate.show(10, false);

            long count = dfFormate.count();

            System.out.println("Nombre total de lignes : " + count);

        } finally {
            spark.stop();
        }
    }
}