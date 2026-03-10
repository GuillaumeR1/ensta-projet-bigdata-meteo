package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

public class DataFormatage {
    private DataFormatage() {
    }

    public static Dataset<Row> formater(Dataset<Row> df) {
        return df
                .withColumnRenamed("NUM_POSTE", "station_id")
                .withColumnRenamed("NOM_USUEL", "station_name")
                .withColumn("timestamp", to_timestamp(col("AAAAMMJJHH"), "yyyyMMddHH"))
                .withColumn("t_c", col("T").cast("double"))
                .withColumn("tn_c", col("TN").cast("double"))
                .withColumn("tx_c", col("TX").cast("double"))
                .withColumn("year", year(col("timestamp")))
                .withColumn("month", month(col("timestamp")))
                .withColumn("day", dayofmonth(col("timestamp")))
                .withColumn("hour", hour(col("timestamp")))
                .select(col("station_id"),
                        col("station_name"),
                        col("LAT"),
                        col("LON"),
                        col("ALTI"),
                        col("timestamp"),
                        col("year"),
                        col("month"),
                        col("day"),
                        col("hour"),
                        col("t_c"),
                        col("tn_c"),
                        col("tx_c"));
    }
}
