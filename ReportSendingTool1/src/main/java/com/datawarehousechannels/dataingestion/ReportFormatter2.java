package com.datawarehousechannels.dataingestion;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ReportFormatter2 {

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub

	

		String ServerConnectionURL = "jdbc:mysql://52.90.244.81:3306/datastore";
		String DBUser = "root";
		String DBPass = "Qwerty12@";
		String DBName = "datastore";
		String TABLENAME = "datawarehouse";	
		String sql = null;
		Connection con = DriverManager.getConnection(ServerConnectionURL,
					DBUser, DBPass);

		BufferedReader br = null;
		Statement st = null;
		
		
		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Reports/AudienceFormatter.csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String line = null;
		HashMap<String, String> AudienceMap = new HashMap<String, String>();
		try {
			while (true) {

				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(",");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						AudienceMap.put(str[0], str[1]);
					}

				}

				else {
					break;
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		     
		     
		     
             for (Map.Entry<String, String> entry : AudienceMap.entrySet())
				{
				    System.out.println(entry.getKey() + "/" + entry.getValue());
				     
				//    sql = "UPDATE datawarehouse SET audience_segment = REPLACE(audience_segment,"+"'uservertical::"+entry.getKey().replace("\"", "")+"' , '"+entry.getValue().replace("\"", "").replace("\'", "")+"' )";
				    System.out.println(sql);
				   
				//	st = con.createStatement();
				//	int effectedRows = st.executeUpdate(sql);
				
				
				}
	
	
           
/*
		     String sql1 = "Select DISTINCT(City)as City from datawarehouse where channel like '%Adwords%'";  
             //Put data in HashSet
	
		     Set<String> adwordcitySet = new HashSet<String>();
		     st = con.createStatement();
		     String citycode = null;
		     
		     
		     ResultSet rs = st.executeQuery(sql1);
				while(rs.next()) {

					   citycode = rs.getString("City");
					   adwordcitySet.add(citycode);
					   System.out.println("Data Fetched successfully:"+citycode);

				}
		                
             
             try {
     			br = new BufferedReader(new FileReader(
     					"C:/Users/sony/Reports/CityFormatter.csv"));
     		} catch (FileNotFoundException e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		}
             
                         
             HashMap<String, String> CityMap = new HashMap<String, String>();
             
            
     			while (true) {

     			try {
     				
     				line = br.readLine();
     				
     				
     				if(line == null || line.isEmpty())
     				break;
     				
     				
     				if (line != null && line.contains("Criteria") == false){
     				//	line=line.replace(",\"",":");
     					
     					line = line.trim();
     				//    System.out.println(line);
     				
     				}
     					
     					if (line != null && !line.isEmpty() && line.contains("Criteria") == false) {
     					String str[] = line.split("\"");
     				//	System.out.println(str[0]);
     					String str1 = str[0];
     					String str2[] = str1.split(",");
     					for (int i = 0; i < 1; i++) {
     						// String arr[] = str[i].split(":");
     						CityMap.put(str2[0], str[1]);
     					  //  System.out.println(str2[0]+":"+str[1]);
     					}

     				

     			//	else {
     				//	break;
     			//	}
     					}

     		}
     					catch (Exception e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		    continue;
     		
     		}

     			}		     	
                  
     			
     			for (Map.Entry<String, String> entry : CityMap.entrySet())
     				{
     				//    System.out.println(entry.getKey() + "/" + entry.getValue());
     				    if(adwordcitySet.contains(entry.getKey())){
     				   
     				    
     				    	sql = "UPDATE datawarehouse SET city = REPLACE(city,'"+entry.getKey()+"' , '"+entry.getValue().replace("\"", "").replace("\'", "")+"' )";
     				    System.out.println(sql);
     				   
     					st = con.createStatement();
     					int effectedRows1 = st.executeUpdate(sql);
     				    }
     				
     				}
             
 */            
             String sql2 = "Select DISTINCT(City) from datawarehouse";  
             String [] cityformat1 = null;
             st = con.createStatement();
             ResultSet rs1 = st.executeQuery(sql2);
				while(rs1.next()) {
					String city1 = rs1.getString("city");
				    cityformat1 = city1.split(","); 
					
				        if(cityformat1.length > 1){
				        sql = "UPDATE datawarehouse SET city = REPLACE(city,'"+city1+"' , '"+cityformat1[0]+"' )";
				        st = con.createStatement();
		     			int effectedRows2 = st.executeUpdate(sql);
				    	
				    	
				    	System.out.println(sql);
				        }
					
					
				 
				}
     			
             String sql1 = "Select City,Country from datawarehouse";  
             //Put data in HashSet
	
		     Set<String> citySet = new HashSet<String>();{
		     Map<String,String> NameMap = new HashMap<String,String>();
		     st = con.createStatement();
		     String citycode = null;
		     String countrycode = null;
		     
		     String cityformat = null;
		     ResultSet rs = st.executeQuery(sql1);
				while(rs.next()) {

					   
					   citycode = rs.getString("City");
					   countrycode = rs.getString("Country");
					   if(!citycode.isEmpty() && citycode != null){
					   citySet.add(countrycode.toLowerCase()+":"+citycode.toLowerCase());
					   NameMap.put(countrycode.toLowerCase()+":"+citycode.toLowerCase(),citycode);
					   System.out.println("Data Fetched successfully:"+citycode);
					   }
				
					   
					   }
		                
             
             try {
     			br = new BufferedReader(new FileReader(
     					"C:/Users/sony/Reports/worldcitiespop.txt"));
     		} catch (FileNotFoundException e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		}
             
                         
             HashMap<String, String> CityMap = new HashMap<String, String>();
             
            
     			while (true) {

     			try {
     				
     				line = br.readLine();
     				
     				
     				if(line == null || line.isEmpty())
     				break;
     				
     		/*		
     				if (line != null && line.contains("Country") == false){
     				//	line=line.replace(",\"",":");
     					
     					line = line.trim();
     				//    System.out.println(line);
     				
     				}
     			*/		
     					if (line != null && !line.isEmpty() && line.contains("Country") == false) {
     					String str[] = line.split(",");
     				//	System.out.println(str[0]);
     					String str1 = str[0];
     					String str2 = str[1];
     					String str3 = str[2];
     					String str4 = str[3];
     					String str5 = str[4];
     					String str6 = str[5];
     					String str7 = str[6];
     			//		String str2[] = str1.split(",");
     					if(citySet.contains(str1.toLowerCase()+":"+str3.toLowerCase())){
     					
     					for (int i = 0; i < 1; i++) {
     						// String arr[] = str[i].split(":");
     						CityMap.put(NameMap.get(str1.toLowerCase() + ":" + str3.toLowerCase()),str3+","+str6+","+str7);
     					  //  System.out.println(str2[0]+":"+str[1]);
     					}
     				}
     				

     			//	else {
     				//	break;
     			//	}
     					}

     		}
     					catch (Exception e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		    continue;
     		
     		}

     			}		     	
                  
     			
     			for (Map.Entry<String, String> entry : CityMap.entrySet())
     				{
     				//    System.out.println(entry.getKey() + "/" + entry.getValue());
     				 //   if(citySet.contains(entry.getKey())){
     				   
     				     String city = entry.getKey();
     				     try{
     				     //   if(entry.getKey().equals("Delhi"))
     				       // 	sql = "UPDATE datawarehouse SET city = REPLACE(city,'"+entry.getKey()+"' , '"+"28.7, 77.1"+"' )";
     				    //    else if(entry.getKey().equals("New Delhi"))
     				     //   	sql = "UPDATE datawarehouse SET city = REPLACE(city,'"+entry.getKey()+"' , '"+"28.6, 77.2"+"' )";
     				    //    else
     				        sql = "UPDATE datawarehouse SET city = REPLACE(city,'"+city+"' , '"+entry.getValue()+"' )";
     				    
     				    	
     				    	
     				    	System.out.println(sql);
     				   
     		     		st = con.createStatement();
     			    	int effectedRows1 = st.executeUpdate(sql);
     				     }
     				     catch(Exception e){
     				    	 continue;
     				    	 
     				     }
     					
     					
     					//   }
     				
     				}
             	
     			
     			
     			
     			
     			
     			
     			
     			
               con.close();
	
	
		     }
	
	}

	}
