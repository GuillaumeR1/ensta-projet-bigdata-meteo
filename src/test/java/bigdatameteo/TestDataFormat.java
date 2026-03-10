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

        try {

            String inputPath = "tmp/data/*.csv.gz";

            Dataset<Row> dfFormate = Main.runPipeline(spark, inputPath);

            dfFormate.printSchema();
            dfFormate.show(10, false);

            long count = dfFormate.count();

            System.out.println("Nombre total de lignes : " + count);

        } finally {
            spark.stop();
        }
    }
}