import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public final class MP3_PartE {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("MP3")
      .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(sc);

    Dataset<?> dataset = FileLoader.loadData(sc, sqlContext);
    dataset.createOrReplaceTempView("gbooks");

    Dataset<Row> df2 = dataset.select("word", "count1").distinct().limit(100);
    df2.createOrReplaceTempView("gbooks2");
    Dataset<Row> df3 = spark.sql("SELECT count(*) FROM gbooks2 t1 JOIN gbooks2 t2 ON t1.count1 = t2.count1");
    df3.show();

    // Finish up
    spark.stop();
    sc.stop();
  }
}
