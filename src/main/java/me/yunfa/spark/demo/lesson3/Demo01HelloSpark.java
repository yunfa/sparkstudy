package me.yunfa.spark.demo.lesson3;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Demo01HelloSpark {

    private static final Logger logger = LoggerFactory.getLogger(Demo01HelloSpark.class);

    public static void initSpark() {
        SparkConf conf = new SparkConf().setMaster("spark://local:7077").setAppName("SparkFirstAPP");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("aa", "bb", "cc"));
        logger.info("lines.count={}", lines.count());
        sc.close();
    }

    public void filterSpark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkFirstAPP");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile(Demo01HelloSpark.class.getResource("/") + "log.txt");
        JavaRDD<String> filterRDD = inputRDD.filter(new Function<String, Boolean>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(String arg0) throws Exception {
                return arg0.contains(" ");
            }
        });
        logger.info("filter result:{}", StringUtils.join(filterRDD.collect(), ","));
        sc.close();
    }

    public static void mapSpark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkFirstAPP");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> map = inputRDD.map(new Function<Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer arg0) throws Exception {
                return arg0 * arg0;
            }
        });
        logger.info("map result:{}", StringUtils.join(map.collect(), ","));
        sc.close();
    }

    public static void flatMapSpark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkDemoAPP");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> wordsRDD = sc.parallelize(Arrays.asList("hello word", "my name", "is li yunfa"));
        wordsRDD.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String arg0) throws Exception {
                return Arrays.asList(arg0.split(" ")).iterator();
            }
        });
        logger.info("wordsRDD fist:");
        printIte(wordsRDD.collect().iterator());
        sc.close();
    }

    public static void printIte(Iterator<?> ite) {
        if (ite != null) {
            while (ite.hasNext()) {
                logger.info("{}", ite.next());
            }
        }
    }

    public static void collectionSpark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> paral1RDD = sc.parallelize(Arrays.asList(111, 333, 444, 222, 444));
        JavaRDD<Integer> paral2RDD = sc.parallelize(Arrays.asList(111, 555, 666, 222, 444));

        // 去重
        JavaRDD<Integer> distinct1RDD = paral1RDD.distinct();
        // 合集
        JavaRDD<Integer> unionRDD = paral1RDD.union(paral2RDD);
        // 并集
        JavaRDD<Integer> intersectionRDD = paral1RDD.intersection(paral2RDD);
        // 在1不在2
        JavaRDD<Integer> subtractRDD = paral1RDD.subtract(paral2RDD);
        // 笛卡尔积
        JavaPairRDD<Integer, Integer> cartesianRDD = paral1RDD.cartesian(paral2RDD);

        logger.info("paral1RDD:");
        printIte(paral1RDD.collect().iterator());
        logger.info("distinct1RDD:");
        printIte(distinct1RDD.collect().iterator());
        logger.info("unionRDD:");
        printIte(unionRDD.collect().iterator());
        logger.info("intersectionRDD:");
        printIte(intersectionRDD.collect().iterator());
        logger.info("subtractRDD:");
        printIte(subtractRDD.collect().iterator());
        logger.info("cartesianRDD:");
        printIte(cartesianRDD.collect().iterator());
        sc.close();
    }

    public static void reduceSpark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduce spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> paralRDD = sc.parallelize(Arrays.asList(11, 22, 33));
        Integer reduceResult = paralRDD.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer arg0, Integer arg1) throws Exception {
                return arg0 + arg1;
            }
        });
        logger.info("reduceResult:{}", reduceResult);
        sc.close();
    }

    public static void main(String[] args) {
        logger.info("hello start .....................");
        // logger.info("split:{}", "hello world".split(" ")[0]);
        // reduceSpark();
        initSpark();
        logger.info("hello  end  .....................");
    }
}
