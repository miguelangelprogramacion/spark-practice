package world.we.deserve;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S02_RDDBasics {

	private static String filein = "/home/animamiguel/Desktop/sparkfiles/lorem.in";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		System.out.println("------------------------------------------------------------------------------------------------");		
		System.out.println("First line with 'consequat':");
		// Load our input data.
		JavaRDD<String> lines = sc.textFile(filein);
		
		JavaRDD<String> linesConsequat = lines.filter(s -> s.contains("consequat"));
		
		System.out.println(linesConsequat.first());
		System.out.println("------------------------------------------------------------------------------------------------");		
		System.out.println("Number of lines with 'consequat': "+linesConsequat.count());
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println("RDD from existing collection");
		
		JavaRDD<String> rddFromCollection = sc.parallelize(Arrays.asList("pandas", "i like pandas", "NOT"));
		JavaRDD<String> linesWithPandas = rddFromCollection.filter(s -> s.contains("pandas"));
		System.out.println("Number of lines with 'pandas': "+linesWithPandas.count());
		
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println("RDD Union");
		JavaRDD<String> union = linesConsequat.union(linesWithPandas);
		System.out.println("Number of lines in union: "+union.count());
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println("RDD Actions: take. Here are 5 examples:");
		union.take(5).forEach(System.out::println);	
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println("RDD Actions: collect. Print all");
		union.collect().forEach(System.out::println);	
	}

}
