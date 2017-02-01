package com.datawarehousechannels.dataingestion;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

public class ChannelDetector {

	public static String detectChannel (String dataline, HashMap<String,String> channelMap){
		
		StringTokenizer tok = new StringTokenizer(dataline, ",",false);
		
		HashMap<String,Integer> channelFinder = new HashMap<String,Integer>();
		
	 	
		String dataunit;
		String channel = null;
		int count = 0;
		String [] channels = null;
		
		while (tok.hasMoreTokens()) {
			dataunit = tok.nextToken();
		    
			   if(dataunit.equals("likes"))
			     return "facebook";
					   
			channel = channelMap.get(dataunit);	
            
			if(channel.contains(",")==true){
              
				channels = channel.split(",");
            
			
			for(int i = 0; i < channels.length; i++){
			
		    if(channelFinder.containsKey(channels[i])==false){
        	  channelFinder.put(channels[i], 1);
        	  
          }
          else
          {
        	 count = channelFinder.get(channels[i]);
        	 channelFinder.put(channels[i],count+1);
          }
			
			}
			
			
	   }
		
			else{
				
				if(channelFinder.containsKey(channel)==false){
		        	  channelFinder.put(channel, 1);
		        	  
		          }
		          else
		          {
		        	 count = channelFinder.get(channel);
		        	 channelFinder.put(channel,count+1);
		          }
					
			}
			
			
		}
	
	
	
	int maxValueInMap = Collections.max(channelFinder.values()); // This
													// will
													// return
													// max
													// value
													// in
													// the
													// Hashmap
	
	
	for (Entry<String, Integer> entry : channelFinder.entrySet()) { // Iterate through
							                                        // hashmap
		if (entry.getValue() == maxValueInMap) {
			channel = entry.getKey(); // Print
										// the
										// key
			                            // with max

			                            // value
		}
	}
	
		
		
		return channel;
		
	}
	

}
