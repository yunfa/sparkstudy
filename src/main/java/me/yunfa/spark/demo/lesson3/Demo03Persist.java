package me.yunfa.spark.demo.lesson3;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Demo03Persist {

    private static Logger logger = LoggerFactory.getLogger(Demo03Persist.class);

    public static void persistMethod() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("appname");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 3, 4, 4));
        JavaRDD<Integer> map = rdd.map(x -> x * x);
        map.persist(StorageLevel.MEMORY_ONLY());
        logger.info("map.count:{}", map.count());
        logger.info("map.collect:{}", map.collect());
        sc.close();
    }

    public static void main(String[] args) {
        persistMethod();
    }
}
