package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static Dataset<Row> runPipeline(SparkSession spark, String inputPath) {

        Dataset<Row> dfBrut = spark.read()
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .csv(inputPath);

        return DataFormatage.formater(dfBrut);
    }
}