package com.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class A {

	public static void main(String[] args) throws FileNotFoundException {
		String cacheFile = "file:/tmp/hadoop-ipieuvre/mapred/local/1443167275967/customers.txt" ;
		BufferedReader joinRead = new BufferedReader(
				new FileReader(cacheFile.toString())) ;
		
		System.out.println(joinRead);
	}

}
