package com.scalawagstudio.emailDirect;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.LocalDateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/*
 * Subscriber data schema:
 * Status, EmailID, CustomFields, Links, ClientName_sws, RunDate_sws, EmailAddress, Created
 */
public class RetrieveSubscribers {
	
	private static final String pageSize = "100";
	private static final String datePattern = "yyyy-MM-dd'T'HH:mm:ssZ'Z'";
	private static final InstanceProfileCredentialsProvider cred = new InstanceProfileCredentialsProvider();
	private static final AmazonS3 s3client = new AmazonS3Client(cred);
	
	public static void main(String[] args) throws ClientProtocolException, IOException {
		
		final Logger LOGGER = LoggerFactory.getLogger(RetrieveSubscribers.class);
		
		final String clientName = args[0];
		final String prefix     = args[1];
		final String rawDir  	= args[2];
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String rsKey = null;
		
		String query = "SELECT apiKey "
				+ "FROM emailClients "
				+ "WHERE emailClient = '"+clientName+"'";

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			conn = DriverManager.getConnection(
					"database_url",
					"username",
					"password");
			stmt = conn.createStatement();	
			rs = stmt.executeQuery(query);
			rs.next();
			rsKey = rs.getString("apiKey");
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | SQLException e1) {
			e1.printStackTrace();
		}
		
		final String apiKey = rsKey.trim();
		
		final String subscriberDir = rawDir.concat("/SubscriberBriefAPI");
		
	  	final LocalDateTime todayDate = new LocalDateTime();
	  	final String todayString = todayDate.toString(datePattern);

	  	
		JSONObject obj = null;
	  	JSONArray items = null;
		int totalPages;
		int skipActive = 0, skipRemove = 0, skipBounce = 0, skipComplaint = 0;
		
		String activeSubscriberURL	= "https://rest.emaildirect.com/v1/Subscribers?PageSize="+pageSize+"&PageNumber=1&ApiKey="+apiKey;
        String removeSubscriberURL	= "https://rest.emaildirect.com/v1/Subscribers/Removes?PageSize="+pageSize+"&PageNumber=1&ApiKey="+apiKey;
		String bounceSubscriberURL = "https://rest.emaildirect.com/v1/Subscribers/Bounces?PageSize="+pageSize+"&PageNumber=1&ApiKey="+apiKey;
		String complaintSubscriberURL = "https://rest.emaildirect.com/v1/Subscribers/Complaints?PageSize="+pageSize+"&PageNumber=1&ApiKey="+apiKey;

		
		//File declaration
		File activeSubscribersCSVFile    = FileUtils.getFile(FileUtils.getUserDirectoryPath() + "/" + prefix + "_Active.csv");
		File removeSubscribersCSVFile  	 = FileUtils.getFile(FileUtils.getUserDirectoryPath() + "/" + prefix + "_Remove.csv");
		File bounceSubscribersCSVFile    = FileUtils.getFile(FileUtils.getUserDirectoryPath() + "/" + prefix + "_Bounce.csv");
		File complaintSubscribersCSVFile = FileUtils.getFile(FileUtils.getUserDirectoryPath() + "/" + prefix + "_Complaint.csv");

	
		
		// Create a CSV file for active subscribers
		try {
			obj = APICall.callAPI(activeSubscriberURL);
		} catch (Exception e) {
			if (e.getMessage().equals("maxAttempts")) {
				System.err.println("Could not get the total pages of active subscriber data.");
				System.exit(1);
			}
		}
		totalPages = APICall.getTotalPages(obj); 
		
		for(int page = 1; page <= totalPages; page++){
			System.out.println("Active\tpage " + page + "/" + totalPages);
			activeSubscriberURL	= "https://rest.emaildirect.com/v1/Subscribers?PageSize="+pageSize+"&PageNumber="+page+"&ApiKey="+apiKey;
			
			try {
				obj = APICall.callAPI(activeSubscriberURL);
				items = obj.getJSONArray("Items");
			} catch (Exception e) {
				if (e.getMessage().equals("maxAttempts")) {
					System.err.println("Skipping a page of active subscriber results.");
		    		skipActive++;
		    		continue;
				}
			}
			items = addSWSFields_Subscriber(items, todayString, clientName);
			APICall.toCSVFile(items, activeSubscribersCSVFile, true);
		}
		APICall.uploadToS3(s3client, subscriberDir, activeSubscribersCSVFile);
		
		
		
		// create a CSV file for removed subscriber
		try {
			obj = APICall.callAPI(removeSubscriberURL); //CallSubscriberAPI() call for remove subscribers
		} catch (Exception e) {
			if (e.getMessage().equals("maxAttempts")) {
				System.err.println("Could not get the total pages of active subscriber data.");
				System.exit(1);
			}
		}
		totalPages = APICall.getTotalPages(obj); //calculate total number of pages
		
		for(int page = 1; page <= totalPages; page++){
			System.out.println("Removes\tpage " + page + "/" + totalPages);
	        removeSubscriberURL	= "https://rest.emaildirect.com/v1/Subscribers/Removes?PageSize="+pageSize+"&PageNumber="+page+"&ApiKey="+apiKey;
	        
        	try {
				obj = APICall.callAPI(removeSubscriberURL);
				items = obj.getJSONArray("Items");
        	} catch (Exception e) {
        		if (e.getMessage().equals("maxAttempts")) {
					System.err.println("Skipping a page of remove subscriber results.");
		    		skipRemove++;
		    		continue;
				}
        	}
        	items = addSWSFields_Subscriber(items, todayString, clientName);
			APICall.toCSVFile(items, removeSubscribersCSVFile, true);
		}
		APICall.uploadToS3(s3client, subscriberDir, removeSubscribersCSVFile);
		
		
		
		// create a CSV file for bounced subscriber
		try {
			obj = APICall.callAPI(bounceSubscriberURL); //CallSubscriberAPI() call for bounce subscribers
		} catch (Exception e) {
			if (e.getMessage().equals("maxAttempts")) {
				System.err.println("Could not get the total pages of active subscriber data.");
				System.exit(1);
			}
		}
		totalPages = APICall.getTotalPages(obj); //calculate total number of pages
		for(int page = 1; page <= totalPages; page++){
			System.out.println("Bounces\tpage " + page + "/" + totalPages);
			bounceSubscriberURL = "https://rest.emaildirect.com/v1/Subscribers/Bounces?PageSize="+pageSize+"&PageNumber="+page+"&ApiKey="+apiKey;
			
			try {
				obj = APICall.callAPI(bounceSubscriberURL);
				items = obj.getJSONArray("Items");
			} catch (Exception e) {
				if (e.getMessage().equals("maxAttempts")) {
					System.err.println("Skipping a page of bounce subscriber results.");
		    		skipBounce++;
		    		continue;
				}
			}
			items = addSWSFields_Subscriber(items, todayString, clientName);
			APICall.toCSVFile(items, bounceSubscribersCSVFile, true);
		}
		APICall.uploadToS3(s3client, subscriberDir, bounceSubscribersCSVFile);
		
		// create a CSV file for complaint subscriber
		try {
			obj = APICall.callAPI(complaintSubscriberURL); //CallSubscriberAPI() call for complaints subscribers
		} catch (Exception e) {
			if (e.getMessage().equals("maxAttempts")) {
				System.err.println("Could not get the total pages of complaint subscriber data");
				System.exit(1);
			}
		}
		totalPages = APICall.getTotalPages(obj); //calculate total number of pages
		for(int page = 1; page <=totalPages; page++){
			System.out.println("Complaints\tpage " + page + "/" + totalPages);
			complaintSubscriberURL = "https://rest.emaildirect.com/v1/Subscribers/Complaints?PageSize="+pageSize+"&PageNumber="+page+"&ApiKey="+apiKey;
			
			try {
				obj = APICall.callAPI(complaintSubscriberURL);
				items = obj.getJSONArray("Items");
			} catch (Exception e) {
				if (e.getMessage().equals("maxAttempts")) {
					System.err.println("Skipping a page of complaint subscriber results.");
		    		skipComplaint++;
		    		continue;
				}
			}
			items = addSWSFields_Subscriber(items, todayString, clientName);
			APICall.toCSVFile(items, complaintSubscribersCSVFile, true);
		} 
		APICall.uploadToS3(s3client, subscriberDir, complaintSubscribersCSVFile);
		
		System.out.println("Skipped "+skipActive+" pages of results for Active due to errors.");
		System.out.println("Skipped "+skipRemove+" pages of results for Remove due to errors.");
		System.out.println("Skipped "+skipBounce+" pages of results for Bounce due to errors.");
		System.out.println("Skipped "+skipComplaint+" pages of results for Complaint due to errors.");
	}
	
	/******************************************************************************************
	 * Insert our own fields into a JSONArray -- there is a version of this method for each 
	 * type of output data since they all have different fields to input
	 * @param array JSONArray to insert our fields into at each index
	 * @param date today's date as a string
	 * @param clientName
	 * @return
	 */
	public static JSONArray addSWSFields_Subscriber(JSONArray array, String date, String clientName) {
		JSONArray arr = array;
		for (int i = 0; i < arr.length(); i++) {
			JSONObject childObject = arr.getJSONObject(i);
			childObject.put("RunDate_sws", date);
			childObject.put("ClientName_sws", clientName);
		}
		return arr;
	}
}
