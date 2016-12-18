package me.yunfa.spark.demo.lesson4;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class Demo04GroupSortJoin {

	private static Logger logger = LoggerFactory.getLogger(Demo04GroupSortJoin.class);

	public static void groupMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> list1 = Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6));
		List<Tuple2<Integer, Integer>> list2 = Arrays.asList(new Tuple2<Integer, Integer>(3, 9));
		JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list1);
		JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(list2);
		JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = rdd1.cogroup(rdd2);
		logger.info("cogroup:{}", cogroup.collect());
		sc.close();
	}

	public static void joinMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> list1 = Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6));
		List<Tuple2<Integer, Integer>> list2 = Arrays.asList(new Tuple2<Integer, Integer>(3, 9));
		JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list1);
		JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(list2);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinRDD = rdd1.join(rdd2);
		logger.info("join:{}", joinRDD.collect());
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightOuterJoinRDD = rdd1.rightOuterJoin(rdd2);
		logger.info("rightOuterJoinRDD:{}", rightOuterJoinRDD.collect());
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> leftOuterJoinRDD = rdd1.leftOuterJoin(rdd2);
		logger.info("leftOuterJoinRDD:{}", leftOuterJoinRDD.collect());
		sc.close();
	}

	public static void countMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> list1 = Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6));
		JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list1);
		Map<Integer, Long> countByKey = rdd1.countByKey();
		for (Entry<Integer, Long> entry : countByKey.entrySet()) {
			logger.info("key:{},val:{}", entry.getKey(), entry.getValue());
		}
		sc.close();
	}

	public static void lookupMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> list1 = Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6));
		JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list1);
		List<Integer> lookup = rdd1.lookup(3);
		logger.info("lookup(3):{}", lookup);
		sc.close();
	}

	public static void main(String[] args) {
		//countMethod();
		lookupMethod();
	}
}
