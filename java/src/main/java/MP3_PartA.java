import java.io.Serializable;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import scala.reflect.ClassTag;
//import java.util.function.Function;

public final class MP3_PartA {

  public static class Record implements Serializable {
    private String word;
    private int count1;
    private int count2;
    private int count3;

    public String getWord() {
      return word;
    }

    public void setWord(String word) {
      this.word = word;
    }

    public int getCount1() {
      return count1;
    }

    public void setCount1(int count1) {
      this.count1 = count1;
    }

    public int getCount2() {
      return count2;
    }

    public void setCount2(int count2) {
      this.count2 = count2;
    }

    public int getCount3() {
      return count3;
    }

    public void setCount3(int count3) {
      this.count3 = count3;
    }
  }

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("MP3")
      .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(sc);
    JavaRDD<Record> record = sc.textFile("gbooks")
      .map(new Function<String, Record>() {
        @Override
        public Record call(String line) throws Exception {
          String[] parts = line.split("\t");
          Record record = new Record();
          record.setWord(parts[0]);
          record.setCount1(Integer.parseInt(parts[1]));
          record.setCount2(Integer.parseInt(parts[2]));
          record.setCount3(Integer.parseInt(parts[3]));
          return record;
        }
      });
    Dataset<?> dataset = sqlContext.applySchema(record, Record.class);
    dataset.printSchema();
    /*
     * 1. Setup (10 points): Download the gbook file and write a function
     * to load it in an RDD & DataFrame
     */

    // RDD API
    // Columns: 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)


    // Spark SQL - DataSet API


    // Finish up
    spark.stop();
    sc.stop();
  }
}
