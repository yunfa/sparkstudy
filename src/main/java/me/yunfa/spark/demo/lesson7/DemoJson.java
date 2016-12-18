package me.yunfa.spark.demo.lesson7;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DemoJson {

	public static void main(String[] args) {
		operateJson();
	}

	public static void operateJson() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("json demo");
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		Dataset<Row> ds = session.read().json("src/main/resources/people.json");
		ds.show();
	}

}
