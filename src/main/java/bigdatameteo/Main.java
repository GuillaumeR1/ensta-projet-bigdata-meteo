package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    private static final String DEFAULT_PARQUET_PATH = "output/parquet";

    public static Dataset<Row> runPipeline(SparkSession spark, String inputPath) {

        Dataset<Row> dfBrut = spark.read()
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .csv(inputPath);

        return DataFormatage.formater(dfBrut);
    }

    public static void main(String[] args) {
        String parquetPath = args.length > 0 ? args[0] : DEFAULT_PARQUET_PATH;

        SparkSession spark = SparkSession.builder()
                .appName("parquet-meteo-analytics")
                .config("spark.driver.memory", "4g")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {
            Dataset<Row> parquetDataset = spark.read().parquet(parquetPath);

            System.out.println("\n=== Releves horaires chauds (T >= 35 C) ===");
            ParquetAnalytics.hotHourlyReadings(parquetDataset).show(200, false);

            System.out.println("\n=== Evolution de la frequence des temperatures >= 35 C au fil des decennies ===");
            ParquetAnalytics.heat35FrequencyByDecade(parquetDataset).show(200, false);

            System.out.println("\n=== Periodes de canicule les plus longues par departement ===");
            ParquetAnalytics.longestHeatwavesByDepartment(parquetDataset).show(200, false);

            System.out.println("\n=== Top 10 des journees les plus chaudes ===");
            ParquetAnalytics.top10HottestDays(parquetDataset).show(10, false);
        } finally {
            spark.stop();
        }
    }
}
