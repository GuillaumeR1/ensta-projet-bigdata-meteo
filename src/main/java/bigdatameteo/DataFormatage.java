package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

public class DataFormatage {

    private DataFormatage() {
    }

    public static Dataset<Row> formater(Dataset<Row> df) {

        Dataset<Row> selected = df.select(
        col("NUM_POSTE"),
        col("NOM_USUEL"),
        col("LAT"),
        col("LON"),
        col("ALTI"),
        col("AAAAMMJJHH"),
        col("T"),
        col("TN"),
        col("TX"),
        col("QT"),
        col("QTN"),
        col("QTX")
        );

        Dataset<Row> cleaned = selected
        .filter(col("T").isNotNull())
        .filter(col("TN").isNotNull())
        .filter(col("TX").isNotNull());

        return cleaned
        .withColumn("timestamp", to_timestamp(col("AAAAMMJJHH"), "yyyyMMddHH"))
        .withColumn("t_c", col("T").cast("double"))
        .withColumn("tn_c", col("TN").cast("double"))
        .withColumn("tx_c", col("TX").cast("double"))
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("departement", substring(col("NUM_POSTE"), 1, 2))
        .select(
        col("NUM_POSTE").alias("station_id"),
        col("NOM_USUEL").alias("station_name"),
        col("departement"),
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
        col("tx_c")
        );
    }
}
