package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;
import org.junit.jupiter.api.Test;

public class TestPerformance {
    private static final int DISPLAY_ROWS = 200;

    @Test
    public void testBenchmarks() {

        SparkSession spark = SparkSession.builder()
        .appName("benchmark-meteo")
        .master("local[*]")
        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String csvPath = "tmp/data/*.csv.gz";
        String avroPath = "output/avro";

        try {

        // Chargement des données CSV et AVRO
            Dataset<Row> dfCsv = Main.runPipeline(spark, csvPath)
            .withColumn("date", to_date(col("timestamp")));

            Dataset<Row> dfAvro = spark.read()
            .format("avro")
            .load(avroPath)
            .withColumn("date", to_date(col("timestamp")));

            // Test 1 : nombre total de jours ou T > 35°C
            System.out.println("\nTEST 1 : nombre total de jours ou T > 35°C");
            benchmarkQuery(
            buildTotalDaysAbove35(dfCsv),
            buildTotalDaysAbove35(dfAvro)
            );

            // Test 2 : jour le plus chaud par departement et annee
            System.out.println("\nTEST 2 : jour le plus chaud par departement et annee");
            benchmarkQuery(
            buildHottestDayByDepartmentAndYear(dfCsv),
            buildHottestDayByDepartmentAndYear(dfAvro)
            );

            // Test 3 : plus longue canicule par departement
            System.out.println("\nTEST 3 : détection des jours de canicule");
            benchmarkQuery(
            buildLongestHeatwaveByDepartment(dfCsv),
            buildLongestHeatwaveByDepartment(dfAvro)
            );

            // Test 4 : top 10 jours les plus chauds
            System.out.println("\nTEST 4 : top 10 jours les plus chauds");
            benchmarkQuery(
            dfCsv.groupBy("departement", "date")
            .agg(max("t_c").alias("temp_max"))
            .orderBy(col("temp_max").desc())
            .limit(10),

            dfAvro.groupBy("departement", "date")
            .agg(max("t_c").alias("temp_max"))
            .orderBy(col("temp_max").desc())
            .limit(10)
            );

        } finally {
            spark.stop();
        }
    }

    private void benchmarkQuery(Dataset<Row> csvQuery, Dataset<Row> avroQuery) {

        long startCsv = System.nanoTime();
        csvQuery.show(DISPLAY_ROWS, false);

        long endCsv = System.nanoTime();
        long startAvro = System.nanoTime();
        avroQuery.show(DISPLAY_ROWS, false);

        long endAvro = System.nanoTime();
        double csvTime = (endCsv - startCsv) / 1_000_000_000.0;
        double avroTime = (endAvro - startAvro) / 1_000_000_000.0;

        System.out.println("\nCSV time  : " + csvTime + " s");
        System.out.println("AVRO time : " + avroTime + " s");
        System.out.println("\n--------------------------------------\n");
    }

    private Dataset<Row> buildTotalDaysAbove35(Dataset<Row> df) {
        return df.filter(col("t_c").gt(35))
        .select("date")
        .distinct()
        .agg(count(lit(1)).alias("nb_jours_t_sup_35"));
    }

    private Dataset<Row> buildHottestDayByDepartmentAndYear(Dataset<Row> df) {
        Dataset<Row> dailyMax = df.groupBy("departement", "year", "date")
        .agg(max("tx_c").alias("tmax"));

        WindowSpec hottestDayWindow = Window.partitionBy("departement", "year")
        .orderBy(col("tmax").desc(), col("date").asc());

        return dailyMax
        .withColumn("rn", row_number().over(hottestDayWindow))
        .filter(col("rn").equalTo(1))
        .select(
        col("year").alias("annee"),
        col("departement"),
        date_format(col("date"), "dd-MM").alias("jour"),
        col("tmax")
        )
        .orderBy(col("annee"), col("departement"));
    }

    private Dataset<Row> buildLongestHeatwaveByDepartment(Dataset<Row> df) {
        Dataset<Row> caniculeDays = buildHeatwaveDays(df);

        WindowSpec sequenceWindow = Window.partitionBy("departement", "year")
        .orderBy(col("date"));

        Dataset<Row> sequencedDays = caniculeDays
        .withColumn("previous_date", lag(col("date"), 1).over(sequenceWindow))
        .withColumn(
        "sequence_start",
        when(
        col("previous_date").isNull().or(datediff(col("date"), col("previous_date")).notEqual(1)),
        1
        ).otherwise(0)
        )
        .withColumn(
        "sequence_id",
        sum("sequence_start").over(sequenceWindow.rowsBetween(Window.unboundedPreceding(), Window.currentRow()))
        );

        Dataset<Row> streaks = sequencedDays
        .groupBy("departement", "year", "sequence_id")
        .agg(count(lit(1)).alias("nb_jours_canicule"));

        WindowSpec longestHeatwaveWindow = Window.partitionBy("departement")
        .orderBy(col("nb_jours_canicule").desc(), col("year").asc());

        return streaks
        .withColumn("rn", row_number().over(longestHeatwaveWindow))
        .filter(col("rn").equalTo(1))
        .select(
        col("departement"),
        col("year").alias("annee"),
        col("nb_jours_canicule")
        )
        .orderBy(col("departement"));
    }

    private Dataset<Row> buildHeatwaveDays(Dataset<Row> df) {
        return df.filter(
        when(col("departement").equalTo("13"),
        col("tx_c").gt(35).and(col("tn_c").gt(24)))
        .when(col("departement").equalTo("33"),
        col("tx_c").gt(35).and(col("tn_c").gt(21)))
        .when(col("departement").equalTo("69"),
        col("tx_c").gt(34).and(col("tn_c").gt(20)))
        .otherwise(false)
        )
        .select("departement", "year", "date")
        .distinct();
    }
}
