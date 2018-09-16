package com.pp.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MoveAverage {
	private static int PERIOD = 3;
	private static Comparator<Tuple2<String, Double>> tupleComparator = new Comparator<Tuple2<String, Double>>() {

		public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
			return o1._1().compareTo(o2._1());
		}
	};

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputPath = TopNUniqueKey.class.getClassLoader().getResource("moveAverage.txt").getPath(); 
	    System.out.println(inputPath);
	    JavaRDD<String> lines = sc.textFile(inputPath,1);
	    JavaPairRDD<String, Tuple2<String, Double>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<String, Double>>() {

			public Tuple2<String, Tuple2<String, Double>> call(String t) throws Exception {
				System.out.println(t);
				String[] tokens = t.split(",");				
				return new Tuple2<String, Tuple2<String,Double>>(
						tokens[0], 
						new Tuple2<String, Double>(tokens[1], new Double(tokens[2])));
			}
		});
	    
	    JavaPairRDD<String, Iterable<Tuple2<String, Double>>> sortedPairs = pairs.groupByKey()
	    		.mapValues(new Function<Iterable<Tuple2<String,Double>>, Iterable<Tuple2<String,Double>>>() {

			public Iterable<Tuple2<String, Double>> call(Iterable<Tuple2<String, Double>> v1) throws Exception {
				List<Tuple2<String, Double>> list = IteratorUtils.toList(v1.iterator());
				//按时间排序
				Collections.sort(list, tupleComparator);
				Queue<Double> queue = new LinkedList<Double>();
				Double sum = 0.00;
				List<Tuple2<String, Double>> newList = new ArrayList<Tuple2<String,Double>>();
				System.out.println(list.toString());
				//计算移动平均
				for (Tuple2<String, Double> t2 : list) {
					sum += t2._2;
					queue.add(t2._2);
					if(queue.size() > PERIOD) {
						sum -= queue.remove();
					}
					newList.add(new Tuple2<String, Double>(t2._1, sum/queue.size()));
				}
				System.out.println(newList.toString());
				return newList;
			}
		});
	    
	    List<Tuple2<String, Iterable<Tuple2<String, Double>>>> result = sortedPairs.collect();
	    sc.close();
	}

}
