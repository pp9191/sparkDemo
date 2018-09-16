package com.pp.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class LeftJoin {

	public static void main(String[] args) {
		leftOuterJoin();
	}

	private static void leftOuterJoin1() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String userPath = LeftJoin.class.getClassLoader().getResource("leftjoinUser.txt").getPath(); 
	    String transactionPath = LeftJoin.class.getClassLoader().getResource("leftjoinTransaction.txt").getPath(); 
	    System.out.println(userPath);
	    System.out.println(transactionPath);
	    
	    JavaRDD<String> users = sc.textFile(userPath,1);
	    JavaRDD<String> transactions = sc.textFile(transactionPath,1);
	    
	    JavaPairRDD<String, Tuple2<String, String>> usersRdd = users.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {

			public Tuple2<String, Tuple2<String, String>> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				Tuple2<String, String> location = new Tuple2<String, String>("L", tokens[1]);
				return new Tuple2<String, Tuple2<String, String>>(tokens[0], location);				
			}
		});
	    
	    JavaPairRDD<String, Tuple2<String, String>> transactionRdd = transactions.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {

			public Tuple2<String, Tuple2<String, String>> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				Tuple2<String, String> product = new Tuple2<String, String>("P", tokens[1]);
				return new Tuple2<String, Tuple2<String, String>>(tokens[2], product);				
			}
		});
	    
	    JavaPairRDD<String, Tuple2<String, String>> allRdd = usersRdd.union(transactionRdd);
	    
	    JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRdd = allRdd.groupByKey();
	    
	    JavaPairRDD<String, String> pairs = groupedRdd.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, String>() {

			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String, String>>> t) throws Exception {
				Iterable<Tuple2<String, String>> it = t._2;
				String location = "";
				List<String> products = new ArrayList<String>();
				for (Tuple2<String, String> t2 : it) {
					if(t2._1.equals("L")) {
						location = t2._2;
					}else {
						products.add(t2._2);
					}
				}
				List<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
				for (String product : products) {
					result.add(new Tuple2<String, String>(product, location));
				}
				return result.iterator();
			}
		});
	    
	    JavaPairRDD<String, Map<String, Integer>> groupedPairs = pairs.groupByKey()
	    		.mapValues(new Function<Iterable<String>, Map<String,Integer>>() {

			public Map<String,Integer> call(Iterable<String> v1) throws Exception {
				Map<String,Integer> map = new HashMap<String, Integer>();
				for (String str : v1) {
					int count = map.containsKey(str) ? map.get(str) : 0;
					map.put(str, count + 1);
				}
				return map;
			}
		});
	    
	    List<Tuple2<String, Map<String, Integer>>> result = groupedPairs.collect();
	    for (Tuple2<String, Map<String, Integer>> tuple2 : result) {
			System.out.println(tuple2._1 + ":::" + tuple2._2.toString());
		}
	    
	    sc.close();
	}

	static void leftOuterJoin() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String userPath = LeftJoin.class.getClassLoader().getResource("leftjoinUser.txt").getPath(); 
	    String transactionPath = LeftJoin.class.getClassLoader().getResource("leftjoinTransaction.txt").getPath(); 
	    System.out.println(userPath);
	    System.out.println(transactionPath);
	    
	    JavaRDD<String> users = sc.textFile(userPath,1);
	    JavaRDD<String> transactions = sc.textFile(transactionPath,1);
	    
	    JavaPairRDD<String, String> userRdd = users.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				return new Tuple2<String, String>(tokens[0], tokens[1]);				
			}
		});
	    
	    JavaPairRDD<String, String> transactionRdd = transactions.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				return new Tuple2<String, String>(tokens[2], tokens[1]);				
			}
		});
	    
	    JavaPairRDD<String, Tuple2<String, Optional<String>>> joinedRdd = transactionRdd.leftOuterJoin(userRdd);
	    
	    JavaPairRDD<String, String> pairs = joinedRdd.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, String, String>() {

			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<String>>> t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(t._2._1, t._2._2.get());
			}
		});
	    
	    JavaPairRDD<String, Map<String, Integer>> groupedPairs = pairs.groupByKey()
	    		.mapValues(new Function<Iterable<String>, Map<String, Integer>>() {

			public Map<String, Integer> call(Iterable<String> v1) throws Exception {
				Map<String,Integer> map = new HashMap<String, Integer>();
				for (String str : v1) {
					int count = map.containsKey(str) ? map.get(str) : 0;
					map.put(str, count + 1);
				}
				return map;
			}
		});
	    
	    List<Tuple2<String, Map<String, Integer>>> result = groupedPairs.collect();
	    for (Tuple2<String, Map<String, Integer>> tuple2 : result) {
			System.out.println(tuple2._1 + ":::" + tuple2._2.toString());
		}
	    
	    sc.close();
	}
}
