package bigdatameteo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataConvertToAvro {

    private DataConvertToAvro() {}

    public static void writeAvro(Dataset<Row> df, String outputPath) {

        df.repartition(8) // limite le nombre de fichier AVRO
        .write()
        .format("avro")
        .mode("overwrite")
        .save(outputPath);
    }

}
