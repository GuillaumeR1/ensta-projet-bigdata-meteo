package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;
import org.junit.jupiter.api.Test;

public class TestPerformance {

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

            // NIVEAU 1 : lecture csv
            Dataset<Row> dfBrut = spark.read()
                    .option("header", "true")
                    .option("sep", ";")
                    .schema(DataSchema.csvSchema())
                    .csv(csvPath);

            Dataset<Row> dfCsv = DataFormatage.formater(dfBrut)
                    .withColumn("date", to_date(col("timestamp")));

            // NIVEAU 2 : lecture avro
            Dataset<Row> dfAvro = spark.read()
                    .format("avro")
                    .load(avroPath)
                    .withColumn("date", to_date(col("timestamp")));



            // TEST 1 : jours > 35°C
            System.out.println("\nTEST 1 : recherche jours où T°C > 35°C");

            benchmarkQuery(
                    dfCsv.filter(col("t_c").gt(35))
                            .select("date", "station_name", "station_id", "t_c")
                            .orderBy(col("t_c").desc())
                            .limit(10),

                    dfAvro.filter(col("t_c").gt(35))
                            .select("date", "station_name", "station_id", "t_c")
                            .orderBy(col("t_c").desc())
                            .limit(10)
            );


            // TEST 2 : jours chauds par département et année
            System.out.println("\nTEST 2 : jour le plus chaud par département et année");

            benchmarkQuery(
                    dfCsv.filter(col("t_c").gt(35))
                            .groupBy("year")
                            .count()
                            .orderBy(col("count").desc())
                            .limit(10),

                    dfAvro.filter(col("t_c").gt(35))
                            .groupBy("year")
                            .count()
                            .orderBy(col("count").desc())
                            .limit(10)
            );


            // TEST 3 : recherche de canicule les plus longues selon les critères de département
            System.out.println("\nTEST 3 : détection des jours de canicule");

            Dataset<Row> caniculeCsv = dfCsv.filter(
                    when(col("departement").equalTo("13"),
                            col("t_c").gt(35).and(col("tn_c").gt(24)))
                            .when(col("departement").equalTo("33"),
                                    col("t_c").gt(35).and(col("tn_c").gt(21)))
                            .when(col("departement").equalTo("69"),
                                    col("t_c").gt(34).and(col("tn_c").gt(20)))
                            .otherwise(false)
            );
            
            Dataset<Row> caniculeAvro = dfAvro.filter(
                    when(col("departement").equalTo("13"),
                            col("t_c").gt(35).and(col("tn_c").gt(24)))
                            .when(col("departement").equalTo("33"),
                                    col("t_c").gt(35).and(col("tn_c").gt(21)))
                            .when(col("departement").equalTo("69"),
                                    col("t_c").gt(34).and(col("tn_c").gt(20)))
                            .otherwise(false)
            );

            benchmarkQuery(
                    caniculeCsv.groupBy("departement", "year")
                            .count()
                            .orderBy(col("count").desc())
                            .limit(10),

                    caniculeAvro.groupBy("departement", "year")
                            .count()
                            .orderBy(col("count").desc())
                            .limit(10)
            );


            // TEST 4 : top 10 jours les plus chauds
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
        csvQuery.show(false);

        long endCsv = System.nanoTime();
        long startAvro = System.nanoTime();
        avroQuery.show(false);

        long endAvro = System.nanoTime();
        double csvTime = (endCsv - startCsv) / 1_000_000_000.0;
        double avroTime = (endAvro - startAvro) / 1_000_000_000.0;

        System.out.println("\nCSV time  : " + csvTime + " s");
        System.out.println("AVRO time : " + avroTime + " s");
        System.out.println("\n--------------------------------------\n");
    }
}