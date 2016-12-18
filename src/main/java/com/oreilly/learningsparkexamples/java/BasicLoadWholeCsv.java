/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import java.io.StringReader;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class BasicLoadWholeCsv {

    public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {

        private static final long serialVersionUID = 1L;

        public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2()));
            return reader.readAll().iterator();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("Usage BasicLoadCsv sparkMaster csvInputFile csvOutputFile key");
        }
        String master = args[0];
        String csvInput = args[1];
        String outputFile = args[2];
        final String key = args[3];

        JavaSparkContext sc = new JavaSparkContext(master, "loadwholecsv", System.getenv("SPARK_HOME"),
                System.getenv("JARS"));
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles(csvInput);
        JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());
        JavaRDD<String[]> result = keyedRDD.filter(new Function<String[], Boolean>() {

            private static final long serialVersionUID = 1L;

            public Boolean call(String[] input) {
                return input[0].equals(key);
            }
        });

        result.saveAsTextFile(outputFile);
        sc.close();
    }
}
