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

            Dataset<Row> dfBrut = spark.read()
            .option("header", "true")
            .option("sep", ";")
            .option("inferSchema", "false")
            .csv(inputPath);

            Dataset<Row> dfFormate = DataFormatage.formater(dfBrut);

            System.out.println("\n***** Schéma du jeu de donnée");
            dfFormate.printSchema();

            System.out.println("\n***** 10 première lignes des données brutes :");
            dfBrut.select("NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI", "AAAAMMJJHH", "T", "TN", "TX")
            .show(10, false);

            System.out.println("\n***** 10 première lignes des données formatées :");
            dfFormate.show(10, false);

            System.out.println("\nNombre total de lignes : " + dfFormate.count());

            } finally {
            spark.stop();
        }
    }
}
