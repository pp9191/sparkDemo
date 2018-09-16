package com.pp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CommonFriend {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = MarketBasketAnalysis.class.getClassLoader().getResource("commonFriend.txt").getPath(); 
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<Tuple2<String, String>, String> pairs = lines.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, String>() {

			public Iterator<Tuple2<Tuple2<String, String>, String>> call(String t) throws Exception {
				String[] token = t.split(":");
				String commonFriend = token[0];
				List<Tuple2<Tuple2<String, String>, String>> result = new ArrayList<Tuple2<Tuple2<String,String>,String>>();
				if(token.length <= 1) {
					return result.iterator();
				}
				String[] friends = token[1].split(",");
				if(friends.length <= 1) {
					return result.iterator();
				}
				List<List<String>> mutualFriends = Combination.findSortedCombinations(Arrays.asList(friends), 2);
				for (List<String> list : mutualFriends) {
					Tuple2<String, String> key = new Tuple2<String, String>(list.get(0), list.get(1));
					result.add(new Tuple2<Tuple2<String,String>, String>(key, commonFriend));
				}				
				return result.iterator();
			}
		});
	    JavaPairRDD<String, Tuple2<String, Iterable<String>>> groupedPairs = pairs.groupByKey()
	    		.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<String,String>,Iterable<String>>, String, Tuple2<String,Iterable<String>>>() {

			public Iterator<Tuple2<String, Tuple2<String, Iterable<String>>>> call(
					Tuple2<Tuple2<String, String>, Iterable<String>> t) throws Exception {
				List<Tuple2<String, Tuple2<String, Iterable<String>>>> result = new ArrayList<Tuple2<String,Tuple2<String,Iterable<String>>>>();
				Tuple2<String, Iterable<String>> value = new Tuple2<String, Iterable<String>>(t._1._2, t._2);
				result.add(new Tuple2<String, Tuple2<String,Iterable<String>>>(t._1._1, new Tuple2<String, Iterable<String>>(t._1._2, t._2)));
				result.add(new Tuple2<String, Tuple2<String,Iterable<String>>>(t._1._2, new Tuple2<String, Iterable<String>>(t._1._1, t._2)));
				return result.iterator();
			}
		});
	    
	    List<Tuple2<String, Tuple2<String, Iterable<String>>>> result = groupedPairs.collect();
	    for (Tuple2<String, Tuple2<String, Iterable<String>>> tuple2 : result) {
			System.out.println(tuple2._1 + "---" + tuple2._2._1 + "===" + tuple2._2._2.toString());
		}
	}

}
