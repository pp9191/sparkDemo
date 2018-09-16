package com.pp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Combination {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> elements = Arrays.asList("A,B,C,D,E,F,G".split(","));
		System.out.println(new Date().getTime());
		List<List<String>> r = findAllSortedCombinations(elements, 7);
		System.out.println(new Date().getTime());
		for (List<String> list : r) {
			System.out.println(list.toString());
		}
		

	}
	
	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n){
		List<List<T>> result = new ArrayList<List<T>>();
		if(n == 0) {
			result.add(new ArrayList<T>());
		}else {
			List<List<T>> combinations = findSortedCombinations(elements, n-1);
			for (List<T> combination : combinations) {
				for (T element : elements) {
					if(!combination.contains(element)) {
						List<T> list = new ArrayList<T>();
						list.addAll(combination);
						list.add(element);
						Collections.sort(list);
						if(!result.contains(list)) {
							result.add(list);
						}
					}
				}
			}
		}
		return result;
	}
	
	public static <T extends Comparable<? super T>> List<List<T>> findAllSortedCombinations(Collection<T> elements, int n){
		if(n==1) {
			return findSortedCombinations(elements, 1);
		}else if(n > 1) {
			List<List<T>> combinations = findAllSortedCombinations(elements, n-1);
			combinations.addAll(findSortedCombinations(elements, n));
			return combinations;
		}
		return null;
	}
	
	public static <T> List<T> removeOne(List<T> list, int i){
		List<T> newList = new ArrayList<T>();
		for (int j = 0; j < list.size(); j++) {
			if(j != i) {
				newList.add(list.get(j));
			}
		}
		return newList;
	}

}
