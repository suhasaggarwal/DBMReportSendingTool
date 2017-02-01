package com.dbm.datadownload;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.http.GenericUrl;
import com.google.api.services.doubleclickbidmanager.DoubleClickBidManager;
import com.google.api.services.doubleclickbidmanager.model.Query;

public class DownloadReportsGoogleCloud {

private static final long CURRENT_REPORT_WINDOW_MS = 12 * 60 * 60 * 1000;
	
	
public static void getReportsForQueryId (long queryId) throws IOException
{

DoubleClickBidManager service = null;
try {
	service = SecurityUtilities.getAPIService();
} catch (Exception e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}	
	
Query queryResponse = service.queries().getquery(queryId).execute();

// If it is recent enough ...
if (queryResponse.getMetadata().getLatestReportRunTimeMs()
    > java.lang.System.currentTimeMillis() - CURRENT_REPORT_WINDOW_MS) {
  //Grab the report.
  GenericUrl reportPath = new GenericUrl(queryResponse.getMetadata()
      .getGoogleCloudStoragePathForLatestReport());
  
  
  OutputStream out = new FileOutputStream(queryResponse.getQueryId() 
      + ".csv");
  MediaHttpDownloader downloader = new MediaHttpDownloader(
      SecurityUtilities.getTransport(), null);
  downloader.download(reportPath, out);
  System.out.println("Download complete.");

}

}



}
