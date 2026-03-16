package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class TestParquet {

    @Test
    public void testParquet() {

        SparkSession spark = SparkSession.builder()
                .appName("test-parquet")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String csvPath = "tmp/data/*.csv.gz";
        String parquetPath = "output/parquet";

        try {
            Dataset<Row> df = Main.runPipeline(spark, csvPath);

            System.out.println("\n***** Schéma des données formatées");
            df.printSchema();
            System.out.println("\n***** Exemple de données formatées");
            df.show(10, false);

            DataConvertToParquet.writeParquet(df, parquetPath);
            Dataset<Row> dfParquet = spark.read()
                    .parquet(parquetPath);
            System.out.println("\n***** Vérification lecture PARQUET");
            dfParquet.printSchema();
            dfParquet.show(10, false);

            long count = dfParquet.count();
            System.out.println("\nNombre total de lignes (PARQUET) : " + count);

        } finally {
            spark.stop();
        }
    }
}