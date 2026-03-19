package bigdatameteo;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
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

    private static final int  DISPLAY_ROWS = 200;
    private static final long TIMEOUT_MS   = 60_000L;

    static class SparkCpuListener extends SparkListener {
        final AtomicLong sparkCpuMs = new AtomicLong(0);

        @Override
        public void onStageCompleted(SparkListenerStageCompleted completed) {
            sparkCpuMs.addAndGet(
                completed.stageInfo().taskMetrics().executorRunTime()
            );
        }

        void reset() { sparkCpuMs.set(0); }
    }

    @Test
    public void testBenchmarks() {

        SparkSession spark = SparkSession.builder()
                .appName("benchmark-meteo")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        SparkCpuListener listener = new SparkCpuListener();
        spark.sparkContext().addSparkListener(listener);

        String csvPath     = "tmp/data/*.csv.gz";
        String avroPath    = "output/avro";
        String parquetPath = "output/parquet";

        try {

            // ════════════════════════════════════════════════════════════════
            // BENCHMARK CSV
            // ════════════════════════════════════════════════════════════════
            System.out.println("\n════════════════════════════════════════════════════════════════");
            System.out.println("  CSV - Spark seul");
            System.out.println("════════════════════════════════════════════════════════════════");

            long csvIoStart = System.nanoTime();
            Dataset<Row> dfCsv = Main.runPipeline(spark, csvPath)
                    .withColumn("date", to_date(col("timestamp")));
            dfCsv.cache();
            dfCsv.count();
            double csvIoSec = (System.nanoTime() - csvIoStart) / 1_000_000_000.0;

            System.out.printf("%n  Lecture I/O : %.3f s%n", csvIoSec);
            System.out.printf("  (inclut parsing CSV + décompression gz + pipeline)%n");

            runAllTests(dfCsv, listener);

            dfCsv.unpersist();

            // ════════════════════════════════════════════════════════════════
            // BENCHMARK AVRO
            // ════════════════════════════════════════════════════════════════
            System.out.println("\n════════════════════════════════════════════════════════════════");
            System.out.println("  AVRO");
            System.out.println("════════════════════════════════════════════════════════════════");

            long avroIoStart = System.nanoTime();
            Dataset<Row> dfAvro = spark.read()
                    .format("avro")
                    .load(avroPath)
                    .withColumn("date", to_date(col("timestamp")));
            dfAvro.cache();
            dfAvro.count();
            double avroIoSec = (System.nanoTime() - avroIoStart) / 1_000_000_000.0;

            System.out.printf("%n  Lecture I/O : %.3f s%n", avroIoSec);

            runAllTests(dfAvro, listener);

            dfAvro.unpersist();

            // ════════════════════════════════════════════════════════════════
            // BENCHMARK PARQUET
            // ════════════════════════════════════════════════════════════════
            System.out.println("\n════════════════════════════════════════════════════════════════");
            System.out.println("  PARQUET");
            System.out.println("════════════════════════════════════════════════════════════════");

            long parquetIoStart = System.nanoTime();
            Dataset<Row> dfParquet = spark.read()
                    .parquet(parquetPath)
                    .withColumn("date", to_date(col("timestamp")));
            dfParquet.cache();
            dfParquet.count();
            double parquetIoSec = (System.nanoTime() - parquetIoStart) / 1_000_000_000.0;

            System.out.printf("%n  Lecture I/O : %.3f s%n", parquetIoSec);

            runAllTests(dfParquet, listener);

            dfParquet.unpersist();

        } finally {
            spark.stop();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Lance les 4 tests sur un DataFrame donné
    // ═══════════════════════════════════════════════════════════════════
    private void runAllTests(Dataset<Row> df, SparkCpuListener listener) {

        System.out.println("\n  TEST 1 : nombre total de jours ou T > 35°C");
        executeWithTimeout(buildTotalDaysAbove35(df), listener);

        System.out.println("\n  TEST 2 : evolution de la frequence des temperatures >= 35 C par decennie");
        executeWithTimeout(ParquetAnalytics.heat35FrequencyByDecade(df), listener);

        System.out.println("\n  TEST 3 : plus longue canicule par departement");
        executeWithTimeout(buildLongestHeatwaveByDepartment(df), listener);

        System.out.println("\n  TEST 4 : top 10 jours les plus chauds");
        executeWithTimeout(
                df.groupBy("departement", "date")
                  .agg(max("t_c").alias("temp_max"))
                  .orderBy(col("temp_max").desc())
                  .limit(10),
                listener
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    // Exécute une query — wall-clock autour de future.get()
    // ═══════════════════════════════════════════════════════════════════
    private void executeWithTimeout(Dataset<Row> query, SparkCpuListener listener) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        listener.reset();

        long start = System.nanoTime();
        Future<?> future = executor.submit(() -> query.show(DISPLAY_ROWS, false));

        try {
            future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            double durationSec = (System.nanoTime() - start) / 1_000_000_000.0;
            long sparkCpuMs = listener.sparkCpuMs.get();

            System.out.printf("  Durée totale  : %.3f s%n", durationSec);
            System.out.printf("  CPU Spark     : %,d ms%n%n", sparkCpuMs);

        } catch (TimeoutException e) {
            future.cancel(true);
            System.out.println("  Timeout (> " + (TIMEOUT_MS / 1000) + "s)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("  Interrompu");
        } catch (ExecutionException e) {
            System.out.println("  Erreur — " + e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Requêtes métier
    // ═══════════════════════════════════════════════════════════════════
    private Dataset<Row> buildTotalDaysAbove35(Dataset<Row> df) {
        return df.filter(col("t_c").gt(35))
                .select("date")
                .distinct()
                .agg(count(lit(1)).alias("nb_jours_t_sup_35"));
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
