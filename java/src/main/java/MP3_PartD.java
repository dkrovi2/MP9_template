import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public final class MP3_PartD {

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("MP3")
      .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(sc);
    Dataset<?> dataset = FileLoader.loadData(sc, sqlContext);
    dataset.createOrReplaceTempView("gbooks");

    Dataset<Row> countDF = spark.sql("" +
      " SELECT word, count(*) " +
      " FROM gbooks " +
      " group by word " +
      " order by count(*) desc, word asc");
    countDF.show(3);
    /*
     * 1. Setup (10 points): Download the gbook file and write a function
     * to load it in an RDD & DataFrame
     */

    // RDD API
    // Columns: 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)


    // Spark SQL - DataSet API


    /*
     * 4. MapReduce (10 points): List the three most frequent 'word' with their count
     * of appearances
     */

    // Dataset/Spark SQL API


    spark.stop();
    sc.stop();
  }
}
