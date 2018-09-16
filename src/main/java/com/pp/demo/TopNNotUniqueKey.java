package com.pp.demo;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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

public class TopNNotUniqueKey {
	private static int SIZE = 5;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = TopNUniqueKey.class.getClassLoader().getResource("topNNotUniqueKey.txt").getPath(); 
	    System.out.println(inputPath);
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String t) throws Exception {
				System.out.println(t);
				String[] tokens = t.split(",");
				return new Tuple2<String, Integer>(tokens[0], new Integer(tokens[1]));
			}
		});
	    JavaPairRDD<String, Integer> uniquePairs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
	    List<Tuple2<String, Integer>> finalTopN = uniquePairs.takeOrdered(SIZE, MyTuple2Comparator.instance);
	    System.out.println(finalTopN.toString());
	}
	
	static class MyTuple2Comparator implements Comparator<Tuple2<String,Integer>>, Serializable{
		
		public static MyTuple2Comparator instance = new MyTuple2Comparator();

		public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
			// TODO Auto-generated method stub
			return (o1._2.compareTo(o2._2) == 0 ? o1._1.compareTo(o2._1) : o1._2.compareTo(o2._2)) * -1;
		}		
	}

}
