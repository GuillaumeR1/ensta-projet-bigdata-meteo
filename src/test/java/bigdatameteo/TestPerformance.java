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

    private static final int  DISPLAY_ROWS = 200;
    private static final long TIMEOUT_MS   = 60_000L;

    // ═══════════════════════════════════════════════════════════════════
    // Listener : cumule le CPU Spark réel via executorRunTime
    // ═══════════════════════════════════════════════════════════════════
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

    // ═══════════════════════════════════════════════════════════════════
    // TESTS
    // ═══════════════════════════════════════════════════════════════════
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

            // ══════════════════════════════════════════════════════════════
            // PHASE I/O — chargement unique, affiché une seule fois
            // Coût fixe à ajouter à la durée totale pour estimer le temps
            // réel d'une première requête sur données froides
            // ══════════════════════════════════════════════════════════════
            System.out.println("\n════════════════════════════════════════════════════════════════");
            System.out.println("  PHASE I/O — Lecture physique + mise en cache (coût unique)");
            System.out.println("════════════════════════════════════════════════════════════════");

            // CSV
            long csvIoStart = System.nanoTime();
            Dataset<Row> dfCsv = Main.runPipeline(spark, csvPath)
                    .withColumn("date", to_date(col("timestamp")));
            dfCsv.cache();
            dfCsv.count();
            double csvIoSec = (System.nanoTime() - csvIoStart) / 1_000_000_000.0;

            // AVRO
            long avroIoStart = System.nanoTime();
            Dataset<Row> dfAvro = spark.read()
                    .format("avro")
                    .load(avroPath)
                    .withColumn("date", to_date(col("timestamp")));
            dfAvro.cache();
            dfAvro.count();
            double avroIoSec = (System.nanoTime() - avroIoStart) / 1_000_000_000.0;

            // PARQUET
            long parquetIoStart = System.nanoTime();
            Dataset<Row> dfParquet = spark.read()
                    .parquet(parquetPath)
                    .withColumn("date", to_date(col("timestamp")));
            dfParquet.cache();
            dfParquet.count();
            double parquetIoSec = (System.nanoTime() - parquetIoStart) / 1_000_000_000.0;

            // Affichage récapitulatif I/O
            System.out.println();
            System.out.printf("  %-10s Lecture I/O : %.3f s%n", "CSV",     csvIoSec);
            System.out.printf("  %-10s Lecture I/O : %.3f s%n", "AVRO",    avroIoSec);
            System.out.printf("  %-10s Lecture I/O : %.3f s%n", "PARQUET", parquetIoSec);
            System.out.println("════════════════════════════════════════════════════════════════\n");

            // ══════════════════════════════════════════════════════════════
            // TESTS — données en cache, I/O ne sera plus relue
            // ══════════════════════════════════════════════════════════════

            System.out.println("TEST 1 : nombre total de jours ou T > 35°C");
            benchmarkQuery(
                    buildTotalDaysAbove35(dfCsv),
                    buildTotalDaysAbove35(dfAvro),
                    buildTotalDaysAbove35(dfParquet),
                    listener
            );

            System.out.println("TEST 2 : jour le plus chaud par departement et annee");
            benchmarkQuery(
                    buildHottestDayByDepartmentAndYear(dfCsv),
                    buildHottestDayByDepartmentAndYear(dfAvro),
                    buildHottestDayByDepartmentAndYear(dfParquet),
                    listener
            );

            System.out.println("TEST 3 : plus longue canicule par departement");
            benchmarkQuery(
                    buildLongestHeatwaveByDepartment(dfCsv),
                    buildLongestHeatwaveByDepartment(dfAvro),
                    buildLongestHeatwaveByDepartment(dfParquet),
                    listener
            );

            System.out.println("TEST 4 : top 10 jours les plus chauds");
            benchmarkQuery(
                    dfCsv.groupBy("departement", "date")
                            .agg(max("t_c").alias("temp_max"))
                            .orderBy(col("temp_max").desc()).limit(10),
                    dfAvro.groupBy("departement", "date")
                            .agg(max("t_c").alias("temp_max"))
                            .orderBy(col("temp_max").desc()).limit(10),
                    dfParquet.groupBy("departement", "date")
                            .agg(max("t_c").alias("temp_max"))
                            .orderBy(col("temp_max").desc()).limit(10),
                    listener
            );

        } finally {
            spark.stop();
        }
    }

    private void benchmarkQuery(Dataset<Row> csvQuery,
                                Dataset<Row> avroQuery,
                                Dataset<Row> parquetQuery,
                                SparkCpuListener listener) {
        executeWithTimeout("CSV",     csvQuery,     listener);
        executeWithTimeout("AVRO",    avroQuery,    listener);
        executeWithTimeout("PARQUET", parquetQuery, listener);
        System.out.println("\n--------------------------------------\n");
    }

    private void executeWithTimeout(String label,
                                    Dataset<Row> query,
                                    SparkCpuListener listener) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        listener.reset();

        long start = System.nanoTime();
        Future<?> future = executor.submit(() -> query.show(DISPLAY_ROWS, false));

        try {
            future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            double durationSec = (System.nanoTime() - start) / 1_000_000_000.0;
            long sparkCpuMs = listener.sparkCpuMs.get();

            System.out.printf("%s :%n", label);
            System.out.printf("  %-20s %.3f s%n", "Durée totale  :", durationSec);
            System.out.printf("  %-20s %,d ms%n",  "CPU Spark     :", sparkCpuMs);
            System.out.println();

        } catch (TimeoutException e) {
            future.cancel(true);
            System.out.println(label + " : Recherche Longue (> " + (TIMEOUT_MS / 1000) + "s)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println(label + " : Recherche interrompue");
        } catch (ExecutionException e) {
            System.out.println(label + " : Erreur lors de l'exécution - " + e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Requêtes métier (inchangées)
    // ═══════════════════════════════════════════════════════════════════
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
