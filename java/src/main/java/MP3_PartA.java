import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public final class MP3_PartA {

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("MP3")
      .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(sc);
    Dataset<?> dataset = FileLoader.loadData(sc, sqlContext);
    dataset.createOrReplaceTempView("gbooks");
    Dataset<Row> result = spark.sql("SELECT word, count1, count2, count3 from gbooks");
    result.printSchema();

    // Finish up
    spark.stop();
    sc.stop();
  }
}
