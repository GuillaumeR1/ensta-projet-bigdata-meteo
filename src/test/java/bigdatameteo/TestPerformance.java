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
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.to_date;
import org.apache.spark.storage.StorageLevel;
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
                .config("spark.driver.memory", "4g")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
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
            dfCsv.persist(StorageLevel.MEMORY_ONLY());
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
            dfAvro.persist(StorageLevel.MEMORY_ONLY());
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
            dfParquet.persist(StorageLevel.MEMORY_ONLY());
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
        executeWithTimeout(ParquetAnalytics.longestHeatwavesByDepartment(df), listener);

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

}
