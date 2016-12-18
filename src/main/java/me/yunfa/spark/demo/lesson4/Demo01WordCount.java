package me.yunfa.spark.demo.lesson4;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class Demo01WordCount {

    private static Logger logger = LoggerFactory.getLogger(Demo01WordCount.class);

    public static void countMethod() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFileRDD = sc.textFile(Demo01WordCount.class.getResource("/") + "log.txt");
        JavaRDD<String> words = textFileRDD.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }
        });
        JavaPairRDD<String, Integer> wordCountRDD = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        logger.info("flatMapRDD:{}", wordCountRDD.collect());
        sc.close();
    }

    public static void countMethod2() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFileRDD = sc.textFile(Demo01WordCount.class.getResource("/") + "log.txt");
        Map<String, Long> countByValueRDD = textFileRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .countByValue();
        logger.info("flatMapRDD:{}", countByValueRDD);
        sc.close();
    }

    public static void main(String[] args) {
        // countMethod();
        countMethod2();
    }

}
