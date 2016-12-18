package me.yunfa.spark.demo.lesson6;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.yunfa.spark.demo.lesson5.Demo01Json;

public class Demo02Accumulator {

	private static Logger logger = LoggerFactory.getLogger(Demo02Accumulator.class);

	public static void accumulatorMethod() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = sc.textFile(Demo01Json.class.getResource("/") + "log.txt");
		final Accumulator<Integer> accu = sc.accumulator(0);
		JavaRDD<String> flatMapRdd = rdd.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				if (line.equals("")) {
					accu.add(1);
				}
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		flatMapRdd.saveAsTextFile("output.txt");
		// logger.info("val:{}", flatMapRdd.collect());
		logger.info("accu values:{}", accu.value());
		sc.close();
	}

	public static void main(String[] args) {
		// TODO 运行会报错，是不是因为没有配置hadoop的原因？
		accumulatorMethod();
	}
}