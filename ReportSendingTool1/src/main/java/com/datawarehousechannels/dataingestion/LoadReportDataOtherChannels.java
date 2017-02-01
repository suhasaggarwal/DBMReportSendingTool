package com.datawarehousechannels.dataingestion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.datawarehousechannels.dataingestion.ChannelDetector;

public class LoadReportDataOtherChannels {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	//	SchemaAndDatacheck.schemacheck();

		HashMap<String, Integer> ColumnKey = new HashMap<String, Integer>();

		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Desktop/migration/universalSchema.txt"));
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
					"C:/Users/sony/Desktop/migration/AdapterWhatsApp.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		line = null;
		HashMap<String, String> AdapterDataWhatsApp = new HashMap<String, String>();
		try {
			while (true) {

				line = br.readLine();
				if (line != null)
					line = line.trim();
				if (line != null && !line.isEmpty()) {
					String str[] = line.split(":");
					for (int i = 0; i < str.length; i++) {
						// String arr[] = str[i].split(":");
						AdapterDataWhatsApp.put(str[0], str[1]);
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

	
		

	
		String [] reports = {"WhatsApp_Report"};
		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Desktop/migration/"+reports[0]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Generate comma separated string with values in appropriate columns

		line = null;
		Integer ColumnNumber = 0;
		String mysqlcolumn;
		String column = null;
		int index = 0;
		int index1 = 0;
		StringBuilder builder = new StringBuilder();
		HashMap<Integer, Integer> ColumnAdapterWhatsApp = new HashMap<Integer, Integer>();
		try {
			while ((line = br.readLine()) != null) {
				StringTokenizer tok = new StringTokenizer(line, ",", false);
				while (tok.hasMoreTokens()) {

					column = tok.nextToken();
					mysqlcolumn = AdapterDataWhatsApp.get(column);
					ColumnNumber = ColumnKey.get(mysqlcolumn);
					if (ColumnNumber != null)
						ColumnAdapterWhatsApp.put(index, ColumnNumber);

					index++;
				}

				break;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	
	
		
	
	
	for(int k= 0; k < reports.length; k++)	
	{
		try {
			br = new BufferedReader(new FileReader(
					"C:/Users/sony/Desktop/migration/"+reports[0]+".csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File fout = null;
		if ( k == 0)
	    fout = new File("C:/Users/sony/Desktop/migration/data_dump_whats_app.csv");
	
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(fout);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		String[] dataline = new String[28];
		String csvline = null;
		for (int i = 0; i < dataline.length; i++) {
			dataline[i] = "" + ",";
		}
		String dataunit = null;
		index = 0;
		index1 = 0;
		int indexmap = 0;

		int count = 0;
		String channel=null;
		try {
			while (true) {

				
				
				index1=0;
				line = br.readLine();
				if (line != null && !line.isEmpty()) {
					
					
										
					if (index != 0) {
						StringTokenizer tok = new StringTokenizer(line, ",",
								false);
						while (tok.hasMoreTokens()) {
							dataunit = tok.nextToken();
							if (k ==0)
							indexmap = ColumnAdapterWhatsApp.get(index1);
							
							
							dataline[indexmap - 1] = dataunit + ",";
							index1++;

						}

					}

					if (index != 0) {

						for (String s : dataline) {
							builder.append(s);
						}
						 
						if(k==0)
						channel = "WhatsApp";
						
						
						builder.append(channel);
						
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

				} else {
					break;
				}
				index++;

			}

			br.close();
			bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
		
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	}

}
