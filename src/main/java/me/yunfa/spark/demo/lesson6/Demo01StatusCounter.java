package me.yunfa.spark.demo.lesson6;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Demo01StatusCounter {

	private static Logger logger = LoggerFactory.getLogger(Demo02Accumulator.class);

	public static void counterMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(2d, 3d, 4d));
		StatCounter statsCounter = rdd.stats();
		logger.info("count:{}", statsCounter.count());
		// 元素的平均值
		logger.info("mean:{}", statsCounter.mean());
		logger.info("sum:{}", statsCounter.sum());
		logger.info("max:{}", rdd.max());
		logger.info("min:{}", rdd.min());
		// 元素的方差
		logger.info("variance:{}", rdd.variance());
		// 从采样中计算出的方差
		logger.info("sampleVariance:{}", rdd.sampleVariance());
		// 标准差
		logger.info("stdev:{}", rdd.stdev());
		// 采样标准差
		logger.info("sampleStdev:{}", rdd.sampleStdev());
		sc.close();
	}

	public static void main(String[] args) {
		counterMethod();
	}
}
