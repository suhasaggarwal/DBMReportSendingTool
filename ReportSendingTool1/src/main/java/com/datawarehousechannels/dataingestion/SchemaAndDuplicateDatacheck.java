package com.datawarehousechannels.dataingestion;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class SchemaAndDuplicateDatacheck {

	
	public static void schemacheck() {

		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/migration/SchemaChange.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String line = null;

		try {
			while (true) {
				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(":");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						DataWarehouseUtil.addColumn(str[0], str[1]);
					}

				} else {
					break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

//Will have to check it at channel level	

	public static boolean duplicatedatacheck(String channel) {
		String Driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://52.90.244.81:3306/datastore";
		String uName = "root";
		String pwd = "Qwerty12@";
		Connection conn = null;
		

		try {
			Class.forName(Driver).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection(url, uName, pwd);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String date = null;
		String advertiser = null;
		Calendar cal = Calendar.getInstance();
		DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		cal.add(Calendar.DATE,-1);
		date = dateformat.format(cal.getTime()).toString();
		
		boolean duplicate = false;
		String sql = "SELECT max(date) AS date FROM DataWarehouse where os="+"'"+channel+"'";
		System.out.println(sql);
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// STEP 5: Extract data from result set
		
		System.out.println(date);
		try {
			System.out.println(rs.getDate("date").toString());
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		try {
			while (rs.next()) {
				
				if (date.equals(rs.getDate("date").toString())) {
					System.out.println(date);
					System.out.println(rs.getDate("date"));
					duplicate = true;
				 System.out.println("Duplicate data will be ignored");
					break;
				}
                   
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return duplicate;
	}

}
