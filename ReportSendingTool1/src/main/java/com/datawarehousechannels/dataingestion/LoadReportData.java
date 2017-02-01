package com.datawarehousechannels.dataingestion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.datawarehousechannels.dataingestion.ChannelDetector;

//Consume reports corresponding to different channels according to their format and ingest in common data warehouse
//After ingestion APIs operate on it to give JSON Feeds



public class LoadReportData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

    	//SchemaAndDuplicateDatacheck.schemacheck();

		
		String filepath = "C:/Users/sony/data/";
		
		HashMap<String, Integer> ColumnKey = new HashMap<String, Integer>();

		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Adapter/universalSchema1.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String line = null;

		boolean duplicatecheck = true;
		// HashMap<String, String> AdapterData = new HashMap<String, String>();
		try {
			while (true) {
				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(":");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						ColumnKey.put(str[0], Integer.parseInt(str[1]));
					}

				} else {
					break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		br = null;

		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Adapter/AdapterFacebook.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		line = null;
		HashMap<String, String> AdapterDataFacebook = new HashMap<String, String>();
		try {
			while (true) {

				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(":");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						AdapterDataFacebook.put(str[0], str[1]);
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

	
		
		br = null;

		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Adapter/AdapterDBM.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		line = null;
		HashMap<String, String> AdapterDataDBM = new HashMap<String, String>();
		try {
			while (true) {

				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(":");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						AdapterDataDBM.put(str[0], str[1]);
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

	
		

		br = null;

		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Adapter/AdapterAdwords.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		line = null;
		HashMap<String, String> AdapterDataAdwords = new HashMap<String, String>();
		try {
			while (true) {

				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(":");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						AdapterDataAdwords.put(str[0], str[1]);
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

	
		String [] reports = {"Facebook_General_Report","DBM_General_Report","Adwords_General_Report"};
		String [] geoReports = {"Facebook_Geo_Report","DBM_GeoDevice_Report","Adwords_GeoDevice_Report"};  
		String [] deviceReports = {"Facebook_Device_Report","DBM_GeoDevice_Report","Adwords_Age_Report"};
		String [] audienceReports = {"Facebook_Audience_Report","DBM_Audience_Report","Adwords_Audience_Report"};
		String [] demographicsReports = {"Facebook_Demographics_Report","DBM_Demographics_Report","Adwords_Gender_Report"};
		
		
		for(int k=0; k < 5; k++)
		{
		
			if(k==0){
			try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Reports/"+reports[0]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

			}
			
		if(k==1){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+geoReports[0]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
			
         if(k==2){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+deviceReports[0]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
         if(k==3){
 			
 			try {
 				br = new BufferedReader(new FileReader(
 						"C:/Users/sony/Reports/"+audienceReports[0]+".csv"));
 			} catch (FileNotFoundException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			}
 		}
 				
         if(k==4){
  			
  			try {
  				br = new BufferedReader(new FileReader(
  						"C:/Users/sony/Reports/"+demographicsReports[0]+".csv"));
  			} catch (FileNotFoundException e) {
  				// TODO Auto-generated catch block
  				e.printStackTrace();
  			}
  		}
  				
          
         
			
			
			// Generate comma separated string with values in appropriate columns

		line = null;
		Integer ColumnNumber = 0;
		String mysqlcolumn;
		String column = null;
		int index = 0;
		int index1 = 0;
		StringBuilder builder = new StringBuilder();
		HashMap<Integer, Integer> ColumnAdapterFacebook = new HashMap<Integer, Integer>();
		try {
			while ((line = br.readLine()) != null) {
				StringTokenizer tok = new StringTokenizer(line, ",", false);
				while (tok.hasMoreTokens()) {

					column = tok.nextToken();
					mysqlcolumn = AdapterDataFacebook.get(column);
					ColumnNumber = ColumnKey.get(mysqlcolumn);
					if (ColumnNumber != null)
						ColumnAdapterFacebook.put(index, ColumnNumber);

					index++;
				}

				break;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	
	

		if(k==0){
		try {
		br = new BufferedReader(new FileReader(
				"C:/Users/sony/Reports/"+reports[1]+".csv"));
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

		}
		
	if(k==1){
		
		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Reports/"+geoReports[1]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
		
     if(k==2){
		
		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Reports/"+deviceReports[1]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
     if(k==3){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+audienceReports[1]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
				
     if(k==4){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+demographicsReports[1]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
				
      
		// Generate comma separated string with values in appropriate columns

		line = null;
		ColumnNumber = 0;
		column = null;
		index = 0;
	    index1 = 0;
		builder = new StringBuilder();
		HashMap<Integer, Integer> ColumnAdapterDBM = new HashMap<Integer, Integer>();
		try {
			while ((line = br.readLine()) != null) {
				StringTokenizer tok = new StringTokenizer(line, ",", false);
				while (tok.hasMoreTokens()) {

					column = tok.nextToken();
					mysqlcolumn = AdapterDataDBM.get(column);
					ColumnNumber = ColumnKey.get(mysqlcolumn);
					if (ColumnNumber != null)
						ColumnAdapterDBM.put(index, ColumnNumber);

					index++;
				}

				break;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	
		if(k==0){
			try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Reports/"+reports[2]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

			}
			
		if(k==1){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+geoReports[2]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
			
	     if(k==2){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+deviceReports[2]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
	     if(k==3){
				
				try {
					br = new BufferedReader(new FileReader(
							"C:/Users/sony/Reports/"+audienceReports[2]+".csv"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
					
	     if(k==4){
				
				try {
					br = new BufferedReader(new FileReader(
							"C:/Users/sony/Reports/"+demographicsReports[2]+".csv"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		// Generate comma separated string with values in appropriate columns

		line = null;
		ColumnNumber = 0;
		column = null;
		index = 0;
	    index1 = 0;
		builder = new StringBuilder();
		HashMap<Integer, Integer> ColumnAdapterAdwords = new HashMap<Integer, Integer>();
		try {
			while ((line = br.readLine()) != null) {
				StringTokenizer tok = new StringTokenizer(line, ",", false);
				while (tok.hasMoreTokens()) {

					column = tok.nextToken();
					mysqlcolumn = AdapterDataAdwords.get(column);
					ColumnNumber = ColumnKey.get(mysqlcolumn);
					if (ColumnNumber != null)
						ColumnAdapterAdwords.put(index, ColumnNumber);

					index++;
				}

				break;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	
	
	for(int m=0; m<3; m++)	{
		
		
		if(k == 0){
		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Reports/"+reports[m]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
		if(k == 1){
		
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+geoReports[m]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
		
		
		
		if(k == 2){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+deviceReports[m]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
		
		if(k == 3){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+audienceReports[m]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
		
		
		if(k == 4){
			
			try {
				br = new BufferedReader(new FileReader(
						"C:/Users/sony/Reports/"+demographicsReports[m]+".csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
		
		File fout = null;
		if ( m == 0)
	    fout = new File("C:/Users/sony/data/data_dump_facebook"+".csv");
		
		if(m == 1)
		fout = new File("C:/Users/sony/data/data_dump_dbm"+".csv");
		
		if ( m == 2)
		fout = new File("C:/Users/sony/data/data_dump_adwords"+".csv");
			
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(fout);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

	    Integer count1 = DataWarehouseUtil.getColumnCount();
		
	    System.out.println(count1);
	    
	    String[] dataline = new String[count1];
		String csvline = null;
		for (int i = 0; i < dataline.length-1; i++) {
			dataline[i] = "" + ",";
		}
	//	String dataunit = null;
		index = 0;
		index1 = 0;
		int indexmap = 0;
    
		
		
		int count = 0;
		String channel="";
		try {
			while (true) {

				
				try{
				
				line = br.readLine();
				
				if(line == null || line.isEmpty())
				break;
				
				StringTokenizer tok1 = new StringTokenizer(line, ",",
						false);
				
				String token = tok1.nextToken();
				
				if(isValidDateformat1(token) || isValidDateformat2(token)){
				
					
					    index1=0;
					//	StringTokenizer tok = new StringTokenizer(line, ",",
						//		false);
				
					//Takes care of empty values    
					    String[] tokens = line.split(",",-1);
					    
					  //  while (tok.hasMoreTokens()) {
						//	dataunit = tok.nextToken();
							
						    for(String dataunit:tokens){	
						
							if (m==0)
							indexmap = ColumnAdapterFacebook.get(index1);
							
							if (m==1)
						    indexmap = ColumnAdapterDBM.get(index1);
							
							if(m==2)
							indexmap = ColumnAdapterAdwords.get(index1);
							
							dataline[indexmap - 1] = dataunit + ",";
							index1++;

						}

						//index1=1;

						for (String s : dataline) {
							builder.append(s);
						}
						 
						if(m==0)
						channel = "Facebook";
						
						if(m==1)
						channel = "DBM";
						
						if(m==2)
						channel = "Adwords";
						
						
						builder.append(channel.trim());
						
						csvline = builder.toString();
				//		duplicatecheck = SchemaAndDatacheck.datacheck(csvline);

				//		if (duplicatecheck == false) {
						//	csvline = csvline.substring(0, csvline.length() - 1);
							System.out.println("CSV LINE:" + csvline + "\n");
							bw.write(csvline);
							bw.newLine();
							builder.setLength(0);
					//	}
							
			  }
				}
				catch(Exception e){
			        e.printStackTrace();
				 	//	continue;
				}
			}
			br.close();
			bw.close();

		    if(m==0){
			
		   // 	SchemaAndDuplicateDatacheck.duplicatedatacheck("Facebook");
		    	LoadReportCSVMysql.importFILE(filepath+"data_dump_facebook.csv");
		    //	boolean success = (new File(filepath+"data_dump_facebook.csv")).delete();
		    }
		    
		    if(m==1){
		    
		    //	SchemaAndDuplicateDatacheck.duplicatedatacheck("DBM");
		    	LoadReportCSVMysql.importFILE(filepath+"data_dump_dbm.csv");
		    //	boolean success = (new File(filepath+"data_dump_dbm.csv")).delete();
		    
		    }		 
		    	 
		    if(m==2){
		    	
		    //    SchemaAndDuplicateDatacheck.duplicatedatacheck("Adwords");
		       LoadReportCSVMysql.importFILE(filepath+"data_dump_adwords.csv");
		    //    boolean success = (new File(filepath+"data_dump_adwords.csv")).delete();
		
		    }
		
		
			
		
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	
	
	}
	
}
	
		
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	}


	public static boolean isValidDateformat1(String inDate) {
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    dateFormat.setLenient(false);
	    try {
	      dateFormat.parse(inDate.trim());
	    } catch (ParseException pe) {
	      return false;
	    }
	    return true;
	  }

 
	public static boolean isValidDateformat2(String inDate) {
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
	    dateFormat.setLenient(false);
	    try {
	      dateFormat.parse(inDate.trim());
	    } catch (ParseException pe) {
	      return false;
	    }
	    return true;
	  }





}
