import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public final class MP3_PartA {

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("MP3")
      .getOrCreate();


    try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
      StructType schema = new StructType()
        .add("word", "string", false)
        .add("count1", "integer", false)
        .add("count2", "integer", false)
        .add("count3", "integer", false);

      // SQLContext sqlContext = new SQLContext(sc);
      // Dataset<?> dataset = FileLoader.loadData(sc, sqlContext);
      Dataset<Row> dataset = spark.read().schema(schema).option("delimiter","\t").load("gbooks");
      dataset.createOrReplaceTempView("gbooks");
      dataset.printSchema();
      sc.stop();
    }

    // Finish up
    spark.stop();
  }
}

// Part A: Your_output_contains:_3_incorrect_lines_in_our_test.Please_try_again!      ;
// Part B: Congrats!_All_lines_are_correct!      ;
// Part C: Congrats!_All_lines_are_correct!      ;
// Part D: Congrats!_All_lines_are_correct!      ;
// Part E: Congrats!_All_lines_are_correct!      ;
