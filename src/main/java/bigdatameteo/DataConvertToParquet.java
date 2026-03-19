package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataConvertToParquet {

    private DataConvertToParquet() {
    }

    public static void writeParquet(Dataset<Row> df, String outputPath) {
        df.coalesce(1)
            .write()
            .mode("overwrite")
            .option("compression", "zstd")
            .parquet(outputPath);
    }
}