package bigdatameteo;

import java.sql.Timestamp;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestParquetAnalytics {

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .appName("test-parquet-analytics")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void shouldAnswerBusinessQuestionsFromParquetOutput() {
        Dataset<Row> dataset = buildDataset();

        List<Row> hotHourlyReadings = ParquetAnalytics.hotHourlyReadings(dataset).collectAsList();
        Assertions.assertEquals(9, hotHourlyReadings.size());
        Assertions.assertEquals(42.0, hotHourlyReadings.get(0).getAs("temperature_c"));

        List<Row> frequencyByDecade = ParquetAnalytics.heat35FrequencyByDecade(dataset).collectAsList();
        Assertions.assertEquals(1, frequencyByDecade.size());
        Assertions.assertEquals(2020, (Integer) frequencyByDecade.get(0).getAs("decennie"));
        Assertions.assertEquals(10L, (Long) frequencyByDecade.get(0).getAs("nb_releves_total"));
        Assertions.assertEquals(9L, (Long) frequencyByDecade.get(0).getAs("nb_releves_ge_35c"));
        Assertions.assertEquals(90.0, ((Number) frequencyByDecade.get(0).getAs("frequence_releves_ge_35c_pct")).doubleValue());

        List<Row> longestHeatwaves = ParquetAnalytics.longestHeatwavesByDepartment(dataset).collectAsList();
        Assertions.assertEquals(2, longestHeatwaves.size());
        Assertions.assertEquals("13", longestHeatwaves.get(0).getAs("departement"));
        Assertions.assertEquals(3L, (Long) longestHeatwaves.get(0).getAs("duree_jours"));
        Assertions.assertEquals("33", longestHeatwaves.get(1).getAs("departement"));
        Assertions.assertEquals(4L, (Long) longestHeatwaves.get(1).getAs("duree_jours"));

        List<Row> top10 = ParquetAnalytics.top10HottestDays(dataset).collectAsList();
        Assertions.assertEquals(10, top10.size());
        Assertions.assertEquals("33", top10.get(0).getAs("departement"));
        Assertions.assertEquals("2024-08-05", top10.get(0).getAs("date"));
        Assertions.assertEquals(42.0, top10.get(0).getAs("temperature_max_c"));
    }

    private Dataset<Row> buildDataset() {
        StructType schema = new StructType()
                .add("station_id", DataTypes.StringType, false)
                .add("station_name", DataTypes.StringType, false)
                .add("departement", DataTypes.StringType, false)
                .add("LAT", DataTypes.StringType, true)
                .add("LON", DataTypes.StringType, true)
                .add("ALTI", DataTypes.StringType, true)
                .add("timestamp", DataTypes.TimestampType, false)
                .add("year", DataTypes.IntegerType, false)
                .add("month", DataTypes.IntegerType, false)
                .add("day", DataTypes.IntegerType, false)
                .add("hour", DataTypes.IntegerType, false)
                .add("t_c", DataTypes.DoubleType, false)
                .add("tn_c", DataTypes.DoubleType, false)
                .add("tx_c", DataTypes.DoubleType, false);

        List<Row> rows = List.of(
                org.apache.spark.sql.RowFactory.create("13001", "Marseille", "13", "0", "0", "0", ts("2024-08-01 14:00:00"), 2024, 8, 1, 14, 36.0, 25.0, 36.0),
                org.apache.spark.sql.RowFactory.create("13001", "Marseille", "13", "0", "0", "0", ts("2024-08-02 14:00:00"), 2024, 8, 2, 14, 37.0, 25.0, 37.0),
                org.apache.spark.sql.RowFactory.create("13001", "Marseille", "13", "0", "0", "0", ts("2024-08-03 14:00:00"), 2024, 8, 3, 14, 38.0, 25.0, 38.0),
                org.apache.spark.sql.RowFactory.create("13001", "Marseille", "13", "0", "0", "0", ts("2024-08-04 14:00:00"), 2024, 8, 4, 14, 34.0, 22.0, 34.0),
                org.apache.spark.sql.RowFactory.create("33001", "Bordeaux", "33", "0", "0", "0", ts("2024-08-05 15:00:00"), 2024, 8, 5, 15, 42.0, 22.0, 42.0),
                org.apache.spark.sql.RowFactory.create("33001", "Bordeaux", "33", "0", "0", "0", ts("2024-08-06 15:00:00"), 2024, 8, 6, 15, 41.0, 22.0, 41.0),
                org.apache.spark.sql.RowFactory.create("33001", "Bordeaux", "33", "0", "0", "0", ts("2024-08-07 15:00:00"), 2024, 8, 7, 15, 40.0, 22.0, 40.0),
                org.apache.spark.sql.RowFactory.create("33001", "Bordeaux", "33", "0", "0", "0", ts("2024-08-08 15:00:00"), 2024, 8, 8, 15, 39.0, 22.0, 39.0),
                org.apache.spark.sql.RowFactory.create("69001", "Lyon", "69", "0", "0", "0", ts("2024-07-01 15:00:00"), 2024, 7, 1, 15, 35.0, 20.0, 35.0),
                org.apache.spark.sql.RowFactory.create("69001", "Lyon", "69", "0", "0", "0", ts("2024-07-02 15:00:00"), 2024, 7, 2, 15, 36.0, 20.0, 36.0)
        );

        return spark.createDataFrame(rows, schema);
    }

    private Timestamp ts(String value) {
        return Timestamp.valueOf(value);
    }
}
