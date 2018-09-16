package com.pp.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SecondarySort {
	
	static Comparator<Tuple2<Integer, Integer>> tupleComparator = new Comparator<Tuple2<Integer, Integer>>() {

		public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
			return o1._1().compareTo(o2._1());
		}
	};

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = SecondarySort.class.getClassLoader().getResource("secondarysort.txt").getPath(); 
	    System.out.println(inputPath);
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

			public Tuple2<String, Tuple2<Integer, Integer>> call(String t) throws Exception {
				System.out.println(t);				
				String[] tokens = t.split("\t");
				return new Tuple2<String, Tuple2<Integer,Integer>>(tokens[0], new Tuple2<Integer, Integer>(new Integer(tokens[1]), new Integer(tokens[2])));
			}
		});
	    JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = pairs
	    		.groupByKey()
	    		.mapValues(new Function<Iterable<Tuple2<Integer,Integer>>, Iterable<Tuple2<Integer,Integer>>>() {

			public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {
				List<Tuple2<Integer, Integer>> list = IteratorUtils.toList(v1.iterator());
				Collections.sort(list, tupleComparator);
				return list;
			}
		});
	    
	    List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> list = sorted.collect();
	    for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> tuple2 : list) {
			System.out.println(tuple2._1() + "\t" + tuple2._2().toString());
		}
	    sc.close();

	}

}
