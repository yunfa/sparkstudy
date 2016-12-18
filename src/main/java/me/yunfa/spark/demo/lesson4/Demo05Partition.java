package me.yunfa.spark.demo.lesson4;

import java.util.Arrays;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class Demo05Partition {

	private static Logger logger = LoggerFactory.getLogger(Demo04GroupSortJoin.class);

	public static void partitionMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(1, 1),
				new Tuple2<Integer, Integer>(2, 2), new Tuple2<Integer, Integer>(3, 3)));
		logger.info("partitioner:{}", rdd.partitioner());
		JavaPairRDD<Integer, Integer> partitionRDD = rdd.partitionBy(new HashPartitioner(2));
		logger.info("partitioner:{}", partitionRDD.partitioner());
		sc.close();
	}

	public static void main(String[] args) {
		partitionMethod();
	}
}
