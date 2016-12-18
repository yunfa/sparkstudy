package me.yunfa.base.tuple;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class LambdaDemo {

	public static void threadDemo() {
		new Thread(() -> {
			System.out.println("thread....");
		}).start();
	}

	public static void listDemo() {
		List<String> list = Arrays.asList("aa", "bb", "cc");
		list.forEach(val -> {
			System.out.println("values=" + val);
		});
	}

	public static void filter(List<String> names, Predicate<String> condition) {
		for (String name : names) {
			if (condition.test(name)) {
				System.out.println(name + " ");
			}
		}
	}

	public static void predicateDemo() {
		List<String> languages = Arrays.asList("Java", "Scala", "C++", "Haskell", "Lisp");

		System.out.println("Languages which starts with J :");
		filter(languages, (str) -> str.startsWith("J"));

		System.out.println("Languages which ends with a ");
		filter(languages, (str) -> str.endsWith("a"));

		System.out.println("Print all languages :");
		filter(languages, (str) -> true);

		System.out.println("Print no language : ");
		filter(languages, (str) -> false);

		System.out.println("Print language whose length greater than 4:");
		filter(languages, (str) -> str.length() > 4);
	}

	public static void mapDemo() {
		List<Integer> list = Arrays.asList(11, 33, 44);
		list.stream().map((cost) -> cost + 10).forEach(System.out::println);
	}

	public static void main(String[] args) {
		// threadDemo();
		// listDemo();
		// predicateDemo();
		mapDemo();
	}
}
