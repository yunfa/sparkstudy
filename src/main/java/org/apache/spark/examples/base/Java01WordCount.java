/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.base;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class Java01WordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder().master("local").appName("Java01WordCount").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile("src/main/resources/log.txt").javaRDD();

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String s) {
				return Arrays.asList(SPACE.split(s)).iterator();
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});

		// 计算字符数量
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		JavaPairRDD<String, Integer> sortCounts = counts.sortByKey();
		List<Tuple2<String, Integer>> output = sortCounts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		// 按value排序，将key跟value反转，再sortByKey
		JavaPairRDD<Integer, String> reverse = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
				return new Tuple2<Integer, String>(stringIntegerTuple2._2(), stringIntegerTuple2._1());
			}
		}).sortByKey(false);
		List<Tuple2<Integer, String>> outputReverse = reverse.collect();
		System.out.println("reverse sort-----------------");
		for (Tuple2<?, ?> tuple : outputReverse) {
			System.out.println(tuple._2() + ": " + tuple._1());
		}

		spark.stop();
	}
}
