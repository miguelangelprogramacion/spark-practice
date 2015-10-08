package world.we.deserve;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.BiFunction;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class S03_CommonTransformationsAndActions {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]").set("spark.executor.memory",
				"1g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = rdd.map(x -> x * x);

		System.out.println("Map / collect");
		System.out.println(StringUtils.join(result.collect(), ","));
		System.out.println("--------------------------------------------------------");
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));

		System.out.println("FlatMap");
		System.out.println("First element " + words.first());
		words.foreach(System.out::println);
		System.out.println("--------------------------------------------------------");

		System.out.println("Reduce");
		JavaRDD<Integer> rddReduce = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		Integer sum = rddReduce.reduce((x, y) -> x + y);
		System.out.println(sum.intValue());
		System.out.println("--------------------------------------------------------");
		System.out.println("Aggregate");
		
		JavaRDD<Integer> rddAggregate = sc.parallelize(Arrays.asList(4,4,4,4));
		AvgCount initial = new AvgCount(0, 0);

		AvgCount resultAggregate = rddAggregate.aggregate(initial, biFunction, combine);
		System.out.println(resultAggregate.avg());

		sc.close();
	}

	static Function2<AvgCount, Integer, AvgCount> biFunction = (a, operand) -> {
		a.total += operand;
		a.num += 1;
		return a;
	};

	static Function2<AvgCount, AvgCount, AvgCount> combine = (a, b) -> {
		a.num += b.num;
		a.total += b.total;
		return a;
	};

}

class AvgCount implements Serializable {
	public AvgCount(int total, int num) {
		this.total = total;
		this.num = num;
	}

	public int total;
	public int num;

	public double avg() {
		return total / (double) num;
	}
}
