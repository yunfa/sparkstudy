package me.yunfa.spark.demo.lesson5;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Demo01Json {

	public static Logger logger = LoggerFactory.getLogger(Demo01Json.class);

	public static void jsonMethod() throws JsonProcessingException {
		Person p1 = new Person(1, "aaa", 12.36, "深圳民治上塘", new Date());
		ObjectMapper mapper = new ObjectMapper();
		String p1Json = mapper.writeValueAsString(p1);
		logger.info("p1Json:{}", p1Json);

		SparkConf conf = new SparkConf().setMaster("local").setAppName("aa");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = sc.textFile(Demo01Json.class.getResource("/") + "person1.json");
		JavaRDD<Person> personRdd = rdd.mapPartitions(new Person());
		logger.info("val:{}", personRdd.collect());
		sc.close();
	}

	public static void main(String[] args) {
		try {
			jsonMethod();
		} catch (JsonProcessingException e) {
			logger.error("json error:", e);
		}
	}
}

class Person implements FlatMapFunction<Iterator<String>, Person> {

	private static final long serialVersionUID = 1L;

	private int pid;

	private String pname;

	private double pmoney;

	private String address;

	private Date birthDay;

	public Person() {

	}

	public Person(int pid, String pname, double pmoney, String address, Date birthDay) {
		super();
		this.pid = pid;
		this.pname = pname;
		this.pmoney = pmoney;
		this.address = address;
		this.birthDay = birthDay;
	}

	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public double getPmoney() {
		return pmoney;
	}

	public void setPmoney(double pmoney) {
		this.pmoney = pmoney;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Date getBirthDay() {
		return birthDay;
	}

	public void setBirthDay(Date birthDay) {
		this.birthDay = birthDay;
	}

	@Override
	public String toString() {
		return "Person [pid=" + pid + ", pname=" + pname + ", pmoney=" + pmoney + ", address=" + address + ", birthDay="
				+ birthDay + "]";
	}

	@Override
	public Iterator<Person> call(Iterator<String> lines) throws Exception {
		ArrayList<Person> people = new ArrayList<Person>();
		ObjectMapper mapper = new ObjectMapper();
		while (lines.hasNext()) {
			String line = lines.next();
			try {
				people.add(mapper.readValue(line, Person.class));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return people.iterator();
	}
}