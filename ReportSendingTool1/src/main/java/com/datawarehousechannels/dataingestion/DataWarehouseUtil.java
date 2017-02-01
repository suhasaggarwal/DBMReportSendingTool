package com.datawarehousechannels.dataingestion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataWarehouseUtil {

//Adds column in Mysql for automatic column addition 
	
	public static void addColumn(String columnName, String columnType) {

		String Driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/datastore";
		String uName = "root";
		String pwd = "";
		Connection conn = null;

		String query = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = "
				+ "'datastore'"
				+ "  AND TABLE_NAME = "
				+ "'DataWarehouse' AND COLUMN_NAME = " + "'" + columnName + "'";

		try {
			Class.forName(Driver).newInstance();
			conn = DriverManager.getConnection(url, uName, pwd);
			Statement stmt1 = conn.createStatement();
			ResultSet rs = stmt1.executeQuery(query);
			if (rs.next() == false) {

				Statement stmt = conn.createStatement();
				String sql = "Alter table DataWarehouse Add" + " " + columnName
						+ " " + columnType;
				stmt.executeUpdate(sql);
				System.out.println("Column added sucessfully");

			} else {

				System.out.println("Column already exist");

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static Integer getColumnCount() {

		String Driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/datastore";
		String uName = "root";
		String pwd = "";
		Connection conn = null;
		Integer count = 0;

		String query = "SELECT COUNT(*) AS COUNT FROM INFORMATION_SCHEMA.COLUMNS WHERE"
				+ " TABLE_NAME = 'datawarehouse'";

		try {
			Class.forName(Driver).newInstance();
			conn = DriverManager.getConnection(url, uName, pwd);
			Statement stmt1 = conn.createStatement();
			System.out.println(query);
			ResultSet rs = stmt1.executeQuery(query);
			if (rs.next()) {
				count = rs.getInt("COUNT");

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return count;

	}

}
