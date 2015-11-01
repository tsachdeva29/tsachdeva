package com.scalawagstudio.emailDirect;


import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/*
 * BeaconClients schema: min, avg, max, samplesCount, client, eventsCount, timestamp
 * BeaconDwelltime schema: maxTime, minTime, session, count, dwellTime, timestamp
 * BeaconRange schema: sourceId, rssis, range, client, value, mac
*/

public class GetBeaconMetrics {

	private static AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
	
	public static void main(String[] args) throws ClientProtocolException, IOException, JSONException, ParseException {
	 
		final Logger LOGGER = LoggerFactory.getLogger(GetBeaconMetrics.class);
		final String accept = args[0];
	  	final String apiKey = args[1];
	  	final String bucketName = args[2];
	  	String startTimeStamp   = args[3];
	  	String endTimeStamp = args[4];
	  	final String beaconDir = bucketName.concat("/Beacon");
	  	final LocalDate todayDate = new LocalDate();

	  	startTimeStamp = startTimeStamp.replace("+", "%2B");
	  	endTimeStamp = endTimeStamp.replace("+", "%2B");
	  	final String clientUrl    = "https://api.kontakt.io/analytics/metrics/clients?sourceId=tz4WC&interval=1h&sourceType=CLOUD_BEACON&iso8601Timestamps=true&startTimestamp="
					+startTimeStamp+"&endTimestamp="+endTimeStamp;
	  	final String dwellTimesUrl = "https://api.kontakt.io/analytics/metrics/dwelltime?sourceId=tz4WC&interval=1h&sourceType=CLOUD_BEACON&iso8601Timestamps=true&startTimestamp="
				+startTimeStamp+"&endTimestamp="+endTimeStamp;
	  	final String rangeUrl = "https://api.kontakt.io/analytics/metrics/ranges?sourceId=4348f354-fd80-41eb-bc8f-28e0aa553393&interval=1h&sourceType=VENUE&iso8601Timestamps=true&startTimestamp=" 
	  			+startTimeStamp+"&endTimestamp="+endTimeStamp;
		File BeaconClientsCSV = FileUtils.getFile(FileUtils.getUserDirectoryPath() +"/"+"BeaconClients_"+todayDate+".csv");
		File BeaconDwellTimesCSV = FileUtils.getFile(FileUtils.getUserDirectoryPath() +"/"+"BeaconDwellTime_"+todayDate+".csv");
		File BeaconRangeCSV = FileUtils.getFile(FileUtils.getUserDirectoryPath() +"/"+"BeaconRange_"+todayDate+".csv");
		
		/*
		 *  Read the Beacon REST API page for Clients
		 */
		JSONObject allBeaconObject = null;
		try {
			allBeaconObject = BeaconAPI.callAPI(clientUrl,accept, apiKey);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		

        JSONArray clients = allBeaconObject.getJSONArray("clients"); 
        
        // Iterate through each of the clients
        // Add the details to the BeaconClients CSV file
        for (int i = 0; i < clients.length(); i++) {
        	JSONObject beaconClient = clients.getJSONObject(i);
        	int samplesCount = beaconClient.getInt("samplesCount");
			int eventsCount = beaconClient.getInt("eventsCount");
			int min = beaconClient.getInt("min");
			int max = beaconClient.getInt("max");
			int avg = beaconClient.getInt("avg");
			String timestamp = beaconClient.getString("timestamp");
			beaconClient.put("client", i);
			
        } 
				
        
        BeaconAPI.toCSVFile(clients, BeaconClientsCSV);
        BeaconAPI.uploadToS3(s3client, beaconDir, BeaconClientsCSV);
        
        /*
		 *  Read the Beacon REST API page for Dwell Time
		 */
        allBeaconObject = null;
		try {
			allBeaconObject = BeaconAPI.callAPI(dwellTimesUrl,accept, apiKey);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
        JSONArray dwellTimes = allBeaconObject.getJSONArray("dwellTimes");
        JSONArray sessions = null;
        // Iterate through the Dwell Times
        // Add the details to the BeaconDwellTimes CSV file
      
        for (int i = 0; i < dwellTimes.length(); i++) {
        	JSONObject dwellTimeObject = dwellTimes.getJSONObject(i);
        	String timestamp = dwellTimeObject.getString("timestamp");
        	sessions = dwellTimeObject.getJSONArray("sessions");
        	int countSession =0;
        	for (int j = 0; j < sessions.length(); j++){
        		countSession++;
        		JSONObject beacondwellTimes = sessions.getJSONObject(j);
        		int maxTime = beacondwellTimes.getInt("maxTime");
        		int minTime = beacondwellTimes.getInt("minTime");
        		int count = beacondwellTimes.getInt("count");
        		beacondwellTimes.put("timestamp", timestamp);
        		beacondwellTimes.put("session", j);
        		beacondwellTimes.put("dwellTime", i);
        	}
    		BeaconAPI.toCSVFile(sessions, BeaconDwellTimesCSV);
        }
        APICall.uploadToS3(s3client, beaconDir, BeaconDwellTimesCSV);
        
        /*
		 *  Read the Beacon REST API page for Dwell Time
		 */
        allBeaconObject = null;
		try {
			allBeaconObject = BeaconAPI.callAPI(rangeUrl,accept, apiKey);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		// Iterate through the Dwell Times
        // Add the details to the BeaconRange CSV file
        JSONArray ranges = allBeaconObject.getJSONArray("ranges");
        JSONArray client = null;
        for (int i = 0; i < ranges.length(); i++) {
        	JSONObject rangeObject = ranges.getJSONObject(i);
        	client = rangeObject.getJSONArray("clients");
        	JSONArray rssis = null;
        	JSONObject clientId = null;
        	String uniqueId =null;
        	String mac =null;
        	boolean checkMac = true;
        	boolean checkUniqueId= true ;
        	for(int j=0;j<client.length(); j++) {
        		System.out.println("client"+i+" rssis"+j);
        		JSONObject clientObject = client.getJSONObject(j);
            	rssis = clientObject.getJSONArray("rssis");
            	if(checkMac = clientObject.getJSONObject("clientId").has("mac")) {
                	mac = clientObject.getJSONObject("clientId").getString("mac");
            	}
            	else if(checkUniqueId = clientObject.getJSONObject("clientId").has("uniqueId")) {
                	uniqueId = clientObject.getJSONObject("clientId").getString("uniqueId");
            	}
            	System.out.println(checkMac + "   "+mac);
            	System.out.println(checkUniqueId+ "   "+uniqueId);
            	System.out.println(rssis);
            	for (int k = 0; k < rssis.length(); k++){
            		JSONObject rssisObject = rssis.getJSONObject(k);
            		String sourceId = rssisObject.getString("sourceId");
            		double value = rssisObject.getDouble("value");
            		rssisObject.put("range", i);
            		rssisObject.put("client",j);
            		rssisObject.put("rssis", k);
            		if(checkMac== true && uniqueId == null) {
                		rssisObject.put("mac", mac);
            		}
            		else if(checkMac == false && checkUniqueId == true) {
            			rssisObject.put("mac", uniqueId);
            		}
            		else if(checkMac == true && checkUniqueId == true) {
                		rssisObject.put("mac", mac);
            		}
            	}
            	BeaconAPI.toCSVFile(rssis, BeaconRangeCSV);
        	}
        }
        APICall.uploadToS3(s3client, beaconDir, BeaconRangeCSV);
        
	}
}