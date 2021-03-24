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
    Dataset<Row> df3 = spark.sql("SELECT * FROM gbooks2 t1 JOIN gbooks2 t2 ON t1.count1 = t2.count1");
    System.out.println(df3.count());

    // Finish up
    spark.stop();
    sc.stop();
  }
}

// Part A: Your_output_contains:_4_incorrect_lines_in_our_test.Please_try_again!      ;
// Part B: Congrats!_All_lines_are_correct!      ;
// Part C: Congrats!_All_lines_are_correct!      ;
// Part D: Congrats!_All_lines_are_correct!      ;
// Part E: Invalid_output.The_number_of_lines_produced_by_your_code_is_not_valid.      ;

