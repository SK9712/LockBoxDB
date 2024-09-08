package com.lockboxdb;

import com.lockboxdb.client.RocksDBClient;


public class Main {

	public static void main(String[] args) {
		try {
			RocksDBClient.getInstance().write("key3", "value3", "String");
			System.out.println(RocksDBClient.getInstance().read("key3", "String"));
			RocksDBClient.getInstance().createTable("testBE");
			RocksDBClient.getInstance().write("testBE", "key1", "value1", "String");
			System.out.println(RocksDBClient.getInstance().read("testBE", "key1", "String"));
			RocksDBClient.getInstance().write("testBE", "key2", "value2", "String");
			System.out.println(RocksDBClient.getInstance().read("testBE", "key2", "String"));
			System.out.println(RocksDBClient.getInstance().read("testBE", "key3", "String"));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
