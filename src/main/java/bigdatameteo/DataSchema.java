package bigdatameteo;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DataSchema {
    private DataSchema() {
    }

    public static StructType csvSchema() {
        return new StructType()
                .add("NUM_POSTE", DataTypes.StringType, true)
                .add("NOM_USUEL", DataTypes.StringType, true)
                .add("LAT", DataTypes.StringType, true)
                .add("LON", DataTypes.StringType, true)
                .add("ALTI", DataTypes.StringType, true)
                .add("AAAAMMJJHH", DataTypes.StringType, true)
                .add("T", DataTypes.StringType, true)
                .add("TN", DataTypes.StringType, true)
                .add("TX", DataTypes.StringType, true);
    }
}
