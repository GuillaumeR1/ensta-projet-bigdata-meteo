package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static Dataset<Row> runPipeline(SparkSession spark, String inputPath) {

        // lecture csv avec le schéma défini dans DataSchema
        Dataset<Row> dfBrut = spark.read()
                .option("header", "true")
                .option("sep", ";")
                .schema(DataSchema.csvSchema())
                .csv(inputPath);

        // application du formatage défini dans DataFormatage
        Dataset<Row> dfFormate = DataFormatage.formater(dfBrut);

        return dfFormate;
    }
}