package me.yunfa.spark.demo.lesson3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Demo02AvgCount implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(Demo02AvgCount.class);

    private static final long serialVersionUID = 1L;

    public int total;

    public int num;

    public Demo02AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public double avg() {
        return this.total / (double) this.num;
    }

    Function2<Demo02AvgCount, Integer, Demo02AvgCount> addAndCount = new Function2<Demo02AvgCount, Integer, Demo02AvgCount>() {

        private static final long serialVersionUID = 1L;

        @Override
        public Demo02AvgCount call(Demo02AvgCount a, Integer x) throws Exception {
            a.total += x;
            a.num += 1;
            return a;
        }
    };

    Function2<Demo02AvgCount, Demo02AvgCount, Demo02AvgCount> combine = new Function2<Demo02AvgCount, Demo02AvgCount, Demo02AvgCount>() {

        private static final long serialVersionUID = 1L;

        @Override
        public Demo02AvgCount call(Demo02AvgCount v1, Demo02AvgCount v2) throws Exception {
            v1.total += v2.total;
            v1.num += v2.num;
            return v1;
        }

    };

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app name");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Demo02AvgCount initial = new Demo02AvgCount(1, 0);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));
        Demo02AvgCount result = rdd.aggregate(initial, initial.addAndCount, initial.combine);
        logger.info("avg:{}", result.avg());
        List<Integer> take = rdd.take(2);
        logger.info("take(1):{}", take);
        logger.info("count:{}", rdd.count());
        logger.info("countByValue:{}", rdd.countByValue());
        logger.info("top(1):{}", rdd.top(3));
        logger.info("takeOrdered(1):{}", rdd.takeOrdered(2));
        logger.info("takeSample(true, 1):{}", rdd.takeSample(true, 1));
        Integer reduce = rdd.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        logger.info("reduce:{}", reduce);
        Integer fold = rdd.fold(0, new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        logger.info("fold:{}", fold);
        rdd.foreach(new VoidFunction<Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Integer v1) throws Exception {
                logger.info("foreach value:{}", v1);
            }
        });
        JavaDoubleRDD mapToDouble = rdd.mapToDouble(new DoubleFunction<Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public double call(Integer v1) throws Exception {
                return v1 * v1;
            }
        });
        logger.debug("mapToDouble:{}", mapToDouble.mean());
        sc.close();
    }

}
