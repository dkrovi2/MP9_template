import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

public class FileLoader {
  private FileLoader() {
    // Not for creating objects
  }

  public static Dataset<?> loadData(final JavaSparkContext sc, final SQLContext sqlContext) {
    JavaRDD<Record> rdd = sc.textFile("gbooks")
      .map(line -> {
        String[] parts = line.split("\t");
        Record record = new Record();
        record.setWord(parts[0]);
        record.setCount1(Integer.parseInt(parts[1]));
        record.setCount2(Integer.parseInt(parts[2]));
        record.setCount3(Integer.parseInt(parts[3]));
        return record;
      });

    return sqlContext.createDataFrame(rdd, Record.class);
  }
}
