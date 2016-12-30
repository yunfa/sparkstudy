package me.yunfa.spark.demo.lesson3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class MapFlatPair {

	private static Logger logger = LoggerFactory.getLogger(MapFlatPair.class);

	public static void main(String[] args) {
		// mapDemo();
		// flatMapDemo();
		// mapPartitionDemo();
		flatMapToPairDemo();
	}

	public static JavaRDD<String> getLines() {
		SparkSession spark = SparkSession.builder().master("local").getOrCreate();
		JavaRDD<String> lines = spark.read().textFile("src/main/resources/mapflat.txt").javaRDD().cache();
		return lines;
	}

	/**
	 * map flatMap flatMapToPair mapPartitions 的区别和用途
	 *
	 * 例如数据是：name：gaoyue age：28
	 *
	 * 方法一：map,我们可以看到数据的每一行在map之后产生了一个数组，那么rdd存储的是一个数组的集合 rdd存储的状态是Array[Array[String]] = Array(Array(name, gaoyue),
	 * Array(age, 28)) Array[String] = Array(name, gaoyue, age, 28)
	 */
	public static void mapDemo() {
		JavaRDD<String[]> mapresult = getLines().map(new Function<String, String[]>() {

			@Override
			public String[] call(String s) throws Exception {
				return s.split(" ");
			}
		});
		List<String[]> collects = mapresult.collect();
		collects.forEach((val) -> {
			logger.debug("length:{},mapDemo:{}", val.length, val);
		});
	}

	/**
	 * 方法二：flatMap 操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个对象 操作2：最后将所有对象合并为一个对象
	 */
	public static void flatMapDemo() {
		JavaRDD<String> objectJavaRDD = getLines().flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});
		objectJavaRDD.collect().forEach(val -> {
			logger.debug("flatMapDemo:{}", val);
		});
	}

	/**
	 * 方法三： mappartition rdd的mapPartitions是map的一个变种，它们都可进行分区的并行处理。两者的主要区别是调用的粒度不一样：
	 * map的输入变换函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区。也就是把每个分区中的内容作为整体来处理的。
	 *
	 */
	public static void mapPartitionDemo() {
		JavaRDD<String> parRDD = getLines().mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			ArrayList<String> results = new ArrayList<String>();

			@Override
			public Iterator<String> call(Iterator<String> s) throws Exception {
				while (s.hasNext()) {
					String tmpStr = s.next();
					logger.debug("next str:{}", tmpStr);
					results.addAll(Arrays.asList(tmpStr.split(" ")));
				}
				return results.iterator();
			}
		});
		parRDD.collect().forEach(val -> {
			logger.debug("mapPartitionDemo:{}", val);
		});
	}

	/**
	 * flatMapToPair 操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个key－value对象 操作2：最后将所有key－value对象合并为一个对象
	 * Iterable<Tuple2<String, String>>
	 *
	 */
	public static void flatMapToPairDemo() {
		JavaPairRDD<String, String> flatMapRDD = getLines().flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String s) throws Exception {
				String[] temp = s.split(" ");
				ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
				list.add(new Tuple2<String, String>(temp[0], temp[1]));
				return list.iterator();
			}
		});
		flatMapRDD.collect().forEach(val -> logger.debug("flatMapToPairDemo:{}", val));
	}
}
