package me.yunfa.spark.demo.lesson4;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class Demo02Pair {

	private static Logger logger = LoggerFactory.getLogger(Demo02Pair.class);

	public static void pairMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> tupleList = Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6));
		JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(tupleList);
		JavaPairRDD<Integer, Integer> reduceRDD = rdd.reduceByKey((x, y) -> x + y);
		logger.info("reduceRdd.collect:{}", reduceRDD.collect());
		JavaPairRDD<Integer, Iterable<Integer>> groupRDD = rdd.groupByKey();
		logger.info("group:{}", groupRDD.collect());
		JavaPairRDD<Integer, Integer> mapValuesRDD = rdd.mapValues(x -> x + 1);
		logger.info("map values:{}", mapValuesRDD.collect());
		JavaRDD<Integer> keysRDD = rdd.keys();
		logger.info("keysRDD:{}", keysRDD.collect());
		JavaRDD<Integer> valuesRDD = rdd.values();
		logger.info("valuesRDD:{}", valuesRDD.collect());
		JavaPairRDD<Integer, Integer> sortByKeyRDD = rdd.sortByKey();
		logger.info("sortByKeyRDD:{}", sortByKeyRDD.collect());
		sc.close();
	}

	public static void pairToPair() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> list1 = Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6));
		List<Tuple2<Integer, Integer>> list2 = Arrays.asList(new Tuple2<Integer, Integer>(3, 9));
		JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list1);
		JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(list2);
		JavaPairRDD<Integer, Integer> subtractByKeyRDD = rdd1.subtractByKey(rdd2);
		logger.info("subtractByKeyRDD:{}", subtractByKeyRDD.collect());
		sc.close();
	}
	
	public static void filterPairValues() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, String>> list = Arrays.asList(new Tuple2<String, String>("holden", "likes coffee"),
				new Tuple2<String, String>("panda", "likes long string and coffee"));
		JavaPairRDD<String, String> rdd = sc.parallelizePairs(list);
		JavaPairRDD<String, String> filterRDD = rdd.filter(new Function<Tuple2<String, String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return v1._2().length() < 20;
			}
		});
		logger.info("filterRDD:{}", filterRDD.collect());
		sc.close();
	}

	public static void main(String[] args) {
		// pairMethod();
		// pairToPair();
		filterPairValues();
	}
}
