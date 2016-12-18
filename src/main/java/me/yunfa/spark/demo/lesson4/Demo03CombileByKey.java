package me.yunfa.spark.demo.lesson4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class Demo03CombileByKey {

	private static Logger logger = LoggerFactory.getLogger(Demo03CombileByKey.class);

	public static void combileMethod() {
		Function<Integer, AvgCount> createCombiner = new Function<Integer, AvgCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public AvgCount call(Integer v1) throws Exception {
				return new AvgCount(v1, 1);
			}
		};

		Function2<AvgCount, Integer, AvgCount> mergeValue = new Function2<AvgCount, Integer, AvgCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public AvgCount call(AvgCount a, Integer x) throws Exception {
				a.total += x;
				a.num += 1;
				return a;
			}
		};

		Function2<AvgCount, AvgCount, AvgCount> mergeCombiners = new Function2<AvgCount, AvgCount, AvgCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public AvgCount call(AvgCount a, AvgCount b) throws Exception {
				a.total += b.total;
				a.num += b.num;
				return a;
			}
		};

		SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> asList = Arrays.asList(new Tuple2<String, Integer>("A", 1),
				new Tuple2<String, Integer>("A", 3), new Tuple2<String, Integer>("B", 4));
		JavaPairRDD<String, Integer> nums = sc.parallelizePairs(asList);
		JavaPairRDD<String, AvgCount> combineByKeyRDD = nums.combineByKey(createCombiner, mergeValue, mergeCombiners);
		Map<String, AvgCount> collectAsMap = combineByKeyRDD.collectAsMap();
		for (Entry<String, AvgCount> entry : collectAsMap.entrySet()) {
			logger.info("key:{},values:{}", entry.getKey(), entry.getValue().getAvg());
		}
		sc.close();
	}

	public static void main(String[] args) {
		combileMethod();
	}

	static class AvgCount implements Serializable {

		private static final long serialVersionUID = 1L;

		public int total;

		public int num;

		public AvgCount(int total, int num) {
			this.total = total;
			this.num = num;
		}

		public float getAvg() {
			return total / (float) num;
		}

		public int getTotal() {
			return total;
		}

		public void setTotal(int total) {
			this.total = total;
		}

		public int getNum() {
			return num;
		}

		public void setNum(int num) {
			this.num = num;
		}
	}
}
