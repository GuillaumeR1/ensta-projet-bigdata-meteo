package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

public class ParquetAnalytics {

    private ParquetAnalytics() {
    }

    public static Dataset<Row> prepare(Dataset<Row> parquetDataset) {
        return parquetDataset.withColumn("date", to_date(col("timestamp")));
    }

    public static Dataset<Row> hotHourlyReadings(Dataset<Row> parquetDataset) {
        return prepare(parquetDataset)
                .filter(col("t_c").geq(35.0))
                .select(
                        col("departement"),
                        col("station_id"),
                        col("station_name"),
                        col("timestamp"),
                        col("t_c").alias("temperature_c")
                )
                .orderBy(col("temperature_c").desc(), col("timestamp").asc());
    }

    public static Dataset<Row> heat35FrequencyByDecade(Dataset<Row> parquetDataset) {
        Dataset<Row> prepared = prepare(parquetDataset)
                .withColumn("decennie", expr("CAST(FLOOR(year / 10) * 10 AS INT)"));

        Dataset<Row> totalReadings = prepared
                .groupBy("decennie")
                .agg(count(lit(1)).alias("nb_releves_total"));

        Dataset<Row> hotReadings = prepared
                .filter(col("t_c").geq(35.0))
                .groupBy("decennie")
                .agg(count(lit(1)).alias("nb_releves_ge_35c"));

        return totalReadings
                .join(hotReadings, new String[]{"decennie"}, "left")
                .na().fill(0, new String[]{"nb_releves_ge_35c"})
                .withColumn(
                        "frequence_releves_ge_35c_pct",
                        round(
                                col("nb_releves_ge_35c")
                                        .multiply(100.0)
                                        .divide(coalesce(col("nb_releves_total"), lit(1))),
                                2
                        )
                )
                .orderBy(col("decennie").asc());
    }

    public static Dataset<Row> longestHeatwavesByDepartment(Dataset<Row> parquetDataset) {
        Dataset<Row> heatwaveDays = buildHeatwaveDays(parquetDataset);

        WindowSpec sequenceWindow = Window.partitionBy("departement", "station_id")
                .orderBy(col("date").asc());

        Dataset<Row> sequenced = heatwaveDays
                .withColumn("previous_date", lag(col("date"), 1).over(sequenceWindow))
                .withColumn(
                        "sequence_start",
                        when(
                                col("previous_date").isNull()
                                        .or(datediff(col("date"), col("previous_date")).notEqual(1)),
                                1
                        ).otherwise(0)
                )
                .withColumn(
                        "sequence_id",
                        sum("sequence_start").over(
                                sequenceWindow.rowsBetween(Window.unboundedPreceding(), Window.currentRow())
                        )
                );

        Dataset<Row> streaks = sequenced
                .groupBy("departement", "station_id", "station_name", "sequence_id")
                .agg(
                        min("date").alias("date_debut"),
                        max("date").alias("date_fin"),
                        count(lit(1)).alias("duree_jours")
                )
                .filter(col("duree_jours").geq(3));

        WindowSpec longestWindow = Window.partitionBy("departement")
                .orderBy(col("duree_jours").desc(), col("date_debut").asc());

        return streaks
                .withColumn("rang", row_number().over(longestWindow))
                .filter(col("rang").equalTo(1))
                .select(
                        col("departement"),
                        col("station_id"),
                        col("station_name"),
                        date_format(col("date_debut"), "yyyy-MM-dd").alias("date_debut"),
                        date_format(col("date_fin"), "yyyy-MM-dd").alias("date_fin"),
                        col("duree_jours")
                )
                .orderBy(col("departement").asc());
    }

    public static Dataset<Row> top10HottestDays(Dataset<Row> parquetDataset) {
        return prepare(parquetDataset)
                .groupBy("departement", "date")
                .agg(max("t_c").alias("temperature_max_c"))
                .orderBy(col("temperature_max_c").desc(), col("date").asc(), col("departement").asc())
                .limit(10)
                .select(
                        col("departement"),
                        date_format(col("date"), "yyyy-MM-dd").alias("date"),
                        col("temperature_max_c")
                );
    }

    private static Dataset<Row> buildHeatwaveDays(Dataset<Row> parquetDataset) {
        Dataset<Row> dailyIndicators = prepare(parquetDataset)
                .groupBy("departement", "station_id", "station_name", "date")
                .agg(
                        max("tx_c").alias("temperature_jour_c"),
                        min("tn_c").alias("temperature_nuit_c")
                );

        return dailyIndicators
                .filter(
                        when(
                                col("departement").equalTo("13"),
                                col("temperature_jour_c").geq(35.0).and(col("temperature_nuit_c").geq(24.0))
                        )
                                .when(
                                        col("departement").equalTo("33"),
                                        col("temperature_jour_c").geq(35.0).and(col("temperature_nuit_c").geq(21.0))
                                )
                                .when(
                                        col("departement").equalTo("69"),
                                        col("temperature_jour_c").geq(34.0).and(col("temperature_nuit_c").geq(20.0))
                                )
                                .otherwise(false)
                )
                .select("departement", "station_id", "station_name", "date");
    }
}
