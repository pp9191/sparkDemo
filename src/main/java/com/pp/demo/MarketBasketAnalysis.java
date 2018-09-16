package com.pp.demo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.pp.demo.TopNNotUniqueKey.MyTuple2Comparator;

import scala.Tuple2;
import scala.Tuple3;

public class MarketBasketAnalysis {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = MarketBasketAnalysis.class.getClassLoader().getResource("marketBasketAnalysis1.txt").getPath(); 
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<List<String>, Integer> pairs = lines.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				if("".equals(v1.trim())) {
					return false;
				}
				return true;
			}
		})
	    		.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {

			public Iterator<Tuple2<List<String>, Integer>> call(String t) throws Exception {
				String[] tokens = t.split(":")[1].split(",");
				List<List<String>> combinations = Combination.findAllSortedCombinations(Arrays.asList(tokens), 3);
				List<Tuple2<List<String>, Integer>> result = new ArrayList<Tuple2<List<String>,Integer>>();
				for (List<String> list : combinations) {
					result.add(new Tuple2<List<String>, Integer>(list, 1));
				}
				return result.iterator();
			}
		});
	    
		JavaPairRDD<List<String>, Integer> combinedPairs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		
		JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subPatterns = combinedPairs.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>,Integer>, List<String>, Tuple2<List<String>, Integer>>() {

			public Iterator<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call(Tuple2<List<String>, Integer> t)
					throws Exception {
				List<String> list = t._1;
				List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<Tuple2<List<String>,Tuple2<List<String>,Integer>>>();
				result.add(new Tuple2<List<String>, Tuple2<List<String>,Integer>>(list, new Tuple2<List<String>, Integer>(null, t._2)));
				if(list.size() == 1) {
					return result.iterator();
				}
				for (int i = 0; i < list.size(); i++) {
					List<String> sublist = Combination.removeOne(list, i);
					result.add(new Tuple2<List<String>, Tuple2<List<String>,Integer>>(sublist, new Tuple2<List<String>, Integer>(list, t._2)));
				}
				return result.iterator();
			}
		});
		
		JavaRDD<List<Tuple3<List<String>, List<String>, Double>>> asscoRules = subPatterns.groupByKey()
				.map(new Function<Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>>, List<Tuple3<List<String>, List<String>, Double>>>() {

			public List<Tuple3<List<String>, List<String>, Double>> call(
					Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> v1) throws Exception {
				List<String> from = v1._1;
				double fromCount = 0;
				List<Tuple2<List<String>, Integer>> toList = new ArrayList<Tuple2<List<String>,Integer>>();
				for (Tuple2<List<String>, Integer> t2 : v1._2) {
					if(t2._1 == null) {
						fromCount = t2._2.doubleValue();
					} else {
						toList.add(t2);
					}
				}
				List<Tuple3<List<String>, List<String>, Double>> result = new ArrayList<Tuple3<List<String>,List<String>,Double>>();
				if(toList.isEmpty()) {
					return result;
				}
				for (Tuple2<List<String>, Integer> t2 : toList) {
					double confidence = t2._2 / fromCount;
					List<String> t2List = new ArrayList<String>(t2._1);
					t2List.removeAll(from);
					result.add(new Tuple3<List<String>, List<String>, Double>(from, t2List, confidence));
				}
				return result;
			}
		});
		
		List<List<Tuple3<List<String>, List<String>, Double>>> result = asscoRules.collect();
		
		for (List<Tuple3<List<String>, List<String>, Double>> list : result) {
			if(!list.isEmpty())
				System.out.println(list.toString());
		}
	}

	private static void basketAnalysis() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = MarketBasketAnalysis.class.getClassLoader().getResource("marketBasketAnalysis1.txt").getPath(); 
	    System.out.println(inputPath);
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<Tuple2<String, String>, Integer> pairs = lines.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, Integer>() {

			public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(String t) throws Exception {
				String[] tokens = t.split(":")[1].split(",");
				List<List<String>> combinations = Combination.findSortedCombinations(Arrays.asList(tokens), 2);
				List<Tuple2<Tuple2<String, String>, Integer>> result = new ArrayList<Tuple2<Tuple2<String,String>,Integer>>();
				for (List<String> list : combinations) {
					result.add(new Tuple2<Tuple2<String,String>, Integer>(
							new Tuple2<String, String>(list.get(0), list.get(1)),1));
				}
				return result.iterator();
			}
		});
	    
	    JavaPairRDD<Tuple2<String, String>, Integer> reducePairs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
	    
	    List<Tuple2<Tuple2<String, String>, Integer>> result = reducePairs.collect();
	    for (Tuple2<Tuple2<String, String>, Integer> tuple2 : result) {
			System.out.println("<" + tuple2._1._1 + "," + tuple2._1._2 + ">\t" + tuple2._2);
		}
	}
	
}
