package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataConvertToParquet {

    private DataConvertToParquet() {
    }

    public static void writeParquet(Dataset<Row> df, String outputPath) {

        Dataset<Row> repartitioned = df.repartition(4, df.col("departement"), df.col("year"));

        repartitioned.write()
                .mode("overwrite")
                .partitionBy("departement", "year")
                .parquet(outputPath);
    }
}