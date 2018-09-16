package com.pp.demo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopNUniqueKey {
	private static int SIZE = 5;

	public static <U> void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = TopNUniqueKey.class.getClassLoader().getResource("topNUniqueKey.txt").getPath(); 
	    System.out.println(inputPath);
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String t) throws Exception {
				System.out.println(t);
				String[] tokens = t.split(",");
				return new Tuple2<String, Integer>(tokens[0], new Integer(tokens[1]));
			}
		});
	    JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>, SortedMap<Integer, String>>() {

			public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {
				SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
				while(t.hasNext()) {
					Tuple2<String, Integer> t2 = t.next();
					top10.put(t2._2, t2._1);
					if(top10.size() > SIZE) {
						top10.remove(top10.firstKey());
					}
				}
				return Collections.singleton(top10).iterator();
			}
		});
	    SortedMap<Integer, String> finalTop10 = partitions.reduce(new Function2<SortedMap<Integer,String>, SortedMap<Integer,String>, SortedMap<Integer,String>>() {
			
			public SortedMap<Integer, String> call(SortedMap<Integer, String> v1, SortedMap<Integer, String> v2)
					throws Exception {
				for (Map.Entry<Integer, String> entry : v2.entrySet()) {
					v1.put(entry.getKey(), entry.getValue());
					if(v1.size() > SIZE) {
						v1.remove(v1.firstKey());
					}
				}
				return v1;
			}
		});
	    System.out.println(finalTop10.toString());

	}

}
