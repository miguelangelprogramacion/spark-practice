package world.we.deserve;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Word count spark. Taken from "Learning Spark - Lightning-Fast Big Data analysis"
 *
 */
public class S01_WordCount {

	private static String filein = "/home/animamiguel/Desktop/sparkfiles/lorem.in";
	private static String fileout = "/home/animamiguel/Desktop/sparkfiles/count.out";
	
	public static void main(String[] args) {
		
		// Create a Java Spark Context
//		SparkConf conf = new SparkConf().setAppName("wordCount");
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load our input data.
		JavaRDD<String> input = sc.textFile(filein);

		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				System.out.println(x);
				return Arrays.asList(x.split(" "));
			}
		});
		// Transform into pairs and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(fileout);
	}
}
