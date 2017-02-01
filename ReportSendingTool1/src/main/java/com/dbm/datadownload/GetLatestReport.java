/*
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.dbm.datadownload;


import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.http.GenericUrl;
import com.google.api.services.doubleclickbidmanager.DoubleClickBidManager;
import com.google.api.services.doubleclickbidmanager.DoubleClickBidManager.Queries.Getquery;
import com.google.api.services.doubleclickbidmanager.model.FilterPair;
import com.google.api.services.doubleclickbidmanager.model.ListQueriesResponse;
import com.google.api.services.doubleclickbidmanager.model.Parameters;
import com.google.api.services.doubleclickbidmanager.model.Query;
import com.google.api.services.doubleclickbidmanager.model.QueryMetadata;
import com.google.api.services.doubleclickbidmanager.model.QuerySchedule;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * Certain Queries are created in DBM system corresponding to different reports such as geography based reports, audience segment based reports,campaign performance reports 
 * Reports are downloaded corresponding to the queries using this module.
 */

public class GetLatestReport {
  // Twelve hours in milliseconds: in this example, reports created after this
 // date are considered fresh.
  private static final long CURRENT_REPORT_WINDOW_MS = 1500 * 24 * 60 * 60 * 1000;

  public static void main(String[] args) throws Exception {
    // Get an authenticated connection to the API.
 
	  
	 DoubleClickBidManager service = SecurityUtilities.getAPIService();
   
 //Creates Query for pulling Customised Report
 /*
	 Query q1 = new Query();
    
    q1.setKind("doubleclickbidmanager#query");

    QueryMetadata metadata = new QueryMetadata();
  
  //Define metadata  
    
    metadata.setTitle("Automatic Data Download");
    metadata.setDataRange("PREVIOUS_DAY");
    
 //Define schedule   
    QuerySchedule schedule = new QuerySchedule();
    schedule.setFrequency("DAILY");
    schedule.setNextRunMinuteOfDay(1 * 60);
   
 
    FilterPair filter = new FilterPair();
 
    
  //Define Filter to be included in Group Bys  
    
    ArrayList<String> filters = new ArrayList<String>();
    filters.add("FILTER_DATE");
    filters.add("FILTER_ADVERTISER");
    filters.add("FILTER_INSERTION_ORDER");
    filters.add("FILTER_LINE_ITEM");
    filters.add("FILTER_ADVERTISER_CURRENCY");
    filters.add("FILTER_CITY");
    filters.add("FILTER_COUNTRY");
    filters.add("FILTER_MOBILE_DEVICE_TYPE");
    filters.add("FILTER_OS");

  //Define Metrics which will be included in Reports  
    
    
    ArrayList<String> metrics = new ArrayList<String>();
    metrics.add("METRIC_IMPRESSIONS");
    metrics.add("METRIC_CLICKS");
    metrics.add("METRIC_CTR");
    metrics.add("METRIC_MEDIA_COST_USD");
    metrics.add("METRIC_REVENUE_USD");
    metrics.add("METRIC_POST_CLICK_DFA_REVENUE");
    metrics.add("METRIC_POST_VIEW_DFA_REVENUE");
    metrics.add("METRIC_BILLABLE_COST_USD");
    metrics.add("METRIC_TOTAL_CONVERSIONS");

 //Define Parameters to be set in Query  
   
    Parameters param = new Parameters();
    
    param.setGroupBys(filters);
    param.setMetrics(metrics);
    q1.setSchedule(schedule);
    q1.setMetadata(metadata);
    q1.setParams(param);
    
  //Set timezone to IST
    
    q1.setTimezoneCode("Asia/Calcutta");
    long unixTime = System.currentTimeMillis(); 
    long value = 1462267171607L;
    q1.setQueryId(unixTime);
  
    
   //Create Query for pulling Report 
   // service.queries().createquery(q1);
   
    //  Getquery q2 = service.queries().getquery(1462270460803L);
   //  q2.
  
    //Run query which will generate Data Report, Data range is a mandatory parameter for running query.
    //  service.queries().runquery(value, Datarange);
    
 //   System.out.println(q2.getQueryId());
    
 //   service.queries(
  //  		).deletequery(q1.getQueryId());
 //   Query queryResponse1 = service.queries().getquery(q2.getQueryId()).execute();
 //   GenericUrl reportPath1 = new GenericUrl(queryResponse1.getMetadata()
      //      .getGoogleCloudStoragePathForLatestReport());
       
   //    System.out.println(reportPath1.toString());
 */   
    
//    long queryId = 1465324387653L;
//      long queryId = 1465382040181L; 
	 
	// long queryId = 1484902693847L;
	 
	// long queryId  = 1484910336722L;
	 
	// long queryId = 1484910720389L;
  //   queryId = 0;

	 
	 long queryId1  = 1485022415751L;
	 long queryId2 = 1485021927402L;
	 DownloadReportwithQueryId(queryId1,1);
	 DownloadReportwithQueryId(queryId2,2);
     String generalReportData = null;
	 String reachReportData = null;
	 String [] generalReport;
	 String [] reachReport;
	 
	 try {
		    BufferedReader in = new BufferedReader(new FileReader("Report2.csv"));
		    String str;
		    int i =0;
		    while ((str = in.readLine()) != null){
		        if(i==1)
		        {
		        reachReport  = str.split(",");	
		        reachReportData = reachReport[5];  	
		        	
		        }
		        i++;
		    }
		        in.close();
		} catch (IOException e) {
		}
	 
	 try {
		    BufferedReader in = new BufferedReader(new FileReader("Report1.csv"));
		    String str;
		    int i = 0;
		    while ((str = in.readLine()) != null){
		        if(i==1){
		        	generalReport = str.split(",");
		        	generalReportData =","+ generalReport[5]+","+generalReport[6]+","+generalReport[7];
		        	
		      }
		        i++;
		    }        
		        in.close();
		} catch (IOException e) {
		}
	
	    BufferedWriter bw = null;
		FileWriter fw = null;

		File file = new File("BritishCouncilReport1.csv");

		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		// true = append file
		
		
		
		
		
		try {

			Calendar cal  = Calendar.getInstance();
		    //subtracting a day
		    cal.add(Calendar.DATE, -2);

		    SimpleDateFormat s = new SimpleDateFormat("yyyy/MM/dd");
		    String result = s.format(new Date(cal.getTimeInMillis()));
			String content = "Cuberoot"+","+result+","+reachReportData+generalReportData+"\n";

			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			bw.write(content);

			System.out.println("Done");

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}
	 
		     SendEmail.sendEmail("dinesh.chand@mslgroup.com,sandip@cuberoot.tech,samreen@cuberoot.tech,nishith1@gmail.com,agrawal.saurabh167@gmail.com,suhas.aggarwal@cuberoot.tech");
		 //  SendEmail.sendEmail("suhas.aggarwal@cuberoot.tech");
  }
  
  
  
  
  /*
	 if (queryId1 == 0) {
      // Call the API, getting a list of queries.
      ListQueriesResponse queryListResponse = service.queries()
          .listqueries().execute();
      // Print them out.
      System.out.println("Id\t\tName");
      for (Query q : queryListResponse.getQueries()) {
        System.out.format("%s\t%s%n", q.getQueryId(), 
            q.getMetadata().getTitle());
      }
    } else{
      // Call the API, getting the latest status for the passed queryId.
      Query queryResponse = service.queries().getquery(queryId).execute();

      // If it is recent enough ...
  //    if (queryResponse.getMetadata().getLatestReportRunTimeMs()
      //    > java.lang.System.currentTimeMillis() - CURRENT_REPORT_WINDOW_MS) {
        // Grab the report.
        
      System.out.println(queryResponse.getMetadata()
            .getGoogleCloudStoragePathForLatestReport());
      
      GenericUrl reportPath = new GenericUrl(queryResponse.getMetadata()
            .getGoogleCloudStoragePathForLatestReport());
       
      
      System.out.println(reportPath);
      
        System.out.println(queryResponse.toPrettyString());
        
        
        System.out.println(reportPath.toString());
        
        OutputStream out = new FileOutputStream("DBM_BritishCouncil_Report2" 
            + ".csv");
        MediaHttpDownloader downloader = new MediaHttpDownloader(
            SecurityUtilities.getTransport(), null);
        downloader.download(reportPath, out);
        System.out.println("Download complete.");
      } 
	 
//	 else {
   //     System.out.format("No reports for query Id %s in the last 12 hours.", 
   //   }
  //  }
  }
*/
  //Give Query Id as Input will Download Report Corresponding to that Query Id - There are different Query ID Corresponding to different Report Type such as for audience,geo,reach reports
 
  protected static void DownloadReportwithQueryId(long queryId, int i){
	  
	 
	  DoubleClickBidManager service = null;
	try {
		service = SecurityUtilities.getAPIService();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
	  // Call the API, getting the latest status for the passed queryId.
      Query queryResponse = null;
	try {
		queryResponse = service.queries().getquery(queryId).execute();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

      // If it is recent enough ...
  //    if (queryResponse.getMetadata().getLatestReportRunTimeMs()
      //    > java.lang.System.currentTimeMillis() - CURRENT_REPORT_WINDOW_MS) {
        // Grab the report.
        
      System.out.println(queryResponse.getMetadata()
            .getGoogleCloudStoragePathForLatestReport());
      
      GenericUrl reportPath = new GenericUrl(queryResponse.getMetadata()
            .getGoogleCloudStoragePathForLatestReport());
       
        try {
			System.out.println(queryResponse.toPrettyString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        System.out.println(reportPath.toString());
        
        OutputStream out = null;
		try {
			out = new FileOutputStream("Report"+i+".csv");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        MediaHttpDownloader downloader = new MediaHttpDownloader(
            SecurityUtilities.getTransport(), null);
        try {
			downloader.download(reportPath, out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("Download complete."); 
	  
	  
  }
  
  
  
  
  protected static Long getQueryId() throws IOException {
    System.out.printf("Enter the query id or press enter to list queries%n");
    String input = readInputLine();
    if (input != null && !input.isEmpty()) {
      return Long.parseLong(input);
    } else {
      return 0L;
    }
  }

  protected static String readInputLine() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    return in.readLine();
  }
}
