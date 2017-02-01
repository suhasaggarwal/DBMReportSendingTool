package com.datawarehousechannels.dataingestion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class LoadReportCSVMysql {

	public static String ServerConnectionURL = "jdbc:mysql://localhost:3306/datastore1";
	public static String DBUser = "root";
	public static String DBPass = "";
	public static String DBName = "datastore1";
	public static String TABLENAME = "datawarehouse";

	public static boolean importFILE(String report) {
	//	String sql = "LOAD DATA LOCAL INFILE '"+report+"' INTO TABLE `" + DBName + "`.`" + TABLENAME + "`";
		
		String sql = "LOAD DATA LOCAL INFILE " +"'"+report+"'"+ " INTO TABLE "+"`" + DBName + "`.`" + TABLENAME + "`"+" FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (`date`,`advertiser`,`campaign_id`,`billableimpression`,`advertiser_status`,`advertiser_integration_code`,`insertion_order`,`insertion_order_id`,`insertion_order_status`,`insertion_order_integration_code`,`line_item`,`line_item_id`,`line_item_status`,`line_item_integration_code`,`targeted_data_providers`,`advertiser_currency`,`impression`,`clicks`,`ctr`,`views`,`view_rate`,`v25`,`v50`,`v75`,`v100`,`likes`,`reach`,`frequency`,`unique_clicks`,`unique_ctr`,`actions`,`people_taking_actions`,`delivery`,`mediacost`,`spent`,`cpc`,`revenue`,`conversions`,`rpc`,`profit`,`timestamp`,`mobilenumber`,`sent`,`delivered`,`opened`,`city`,`country`,`audience_segment`,`audience_list_id`,`audience_list_type`,`audience_list_cost`,`age`,`gender`,`mobile_brandname`,`mobile_model`,`screen_resolution`,`device_type`,`browser`,`os`,`city_id`,`Uniq_Cookies`,`channel`)"; 
		System.out.println(sql);
		boolean valid = true;
		String errorMessage = null;
		int error;
		try {
			Connection con = DriverManager.getConnection(ServerConnectionURL,
					DBUser, DBPass);
			Statement st = con.createStatement();
			int effectedRows = st.executeUpdate(sql);
			if (effectedRows == 0) {
				error = 0;
				valid = false;
				errorMessage = errorMessage
						+ "\n Error: running "
						+ TABLENAME
						+ " migration: app not able to import data from file to table ";
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception in Importfile: Data Migration");
			valid = false;
		}
		return valid;
	}

}
