package com.scalawagstudio.emailDirect;


import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/*
 * campaignDetail schema:
 * CTR, Removes, Opens, IsActive, Forwards, ForwardsFrom, SoftBounceRate,
 * ScheduleDate, Name, Created, FromName, Target, UniqueRate, Creative,
 * EmailsSent, Complaints, ToName, HardBounces, Status, TotalClicks, Delivered,
 * DeliveryRate, Date_sws, Publication, ClientName_sws, Subject, OpenRate, RemoveRate,
 * BounceRate, CampaignID, SoftBounces, UniqueClicks, Links, ArchiveURL, ComplaintRate, FromEmail
 */
public class RetrieveCampaignResults {
	
	private static final String pageSize = "100";
	private static final String datePattern = "yyyyMMdd";
	private static final InstanceProfileCredentialsProvider cred = new InstanceProfileCredentialsProvider();
	private static final AmazonS3 s3client = new AmazonS3Client(cred);
	
	public static void main(String[] args) throws ClientProtocolException, IOException, JSONException, ParseException {
	 
		final Logger LOGGER = LoggerFactory.getLogger(RetrieveCampaignResults.class);
		
	  	final String clientName = args[0];
	  	final String prefix     = args[1];
	  	final String rawDir  	= args[2];
	  	final int maxAge =   (args.length == 4) ? Integer.parseInt(args[3]) : 30;
	  	
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

	  	final String emailEventDir = rawDir.concat("/campaignEmailEvent");
	  	final String emailDetailDir = rawDir.concat("/campaignDetail");
	  	final String linkDir = rawDir.concat("/link");
	  	final String linkDetailDir = rawDir.concat("/linkDetail");
	  	
	  	final LocalDate todayDate = new LocalDate();
	  	final LocalDate sinceDate = todayDate.minusDays(maxAge);
	  	String todayString = todayDate.toString(datePattern);
	  	//String since = (maxAge > 0) ? sinceDate.toString("MM/dd/yyyy") : "01/01/1970"; This would be useful to JUST pull the past 30 days of 
	  																					// campaigns... but we're pulling all campaigns then filtering
	  	
	  	
        String campaignName ="";
        int maxPage = 0;
        int campaignID;
        int skipCampaignPageCount = 0, skipCampaignCount = 0, skipLinkCount = 0, skipEventCount = 0;
        List<Integer> campaignIdList = new ArrayList<Integer>();

        
		File campaignDetailsFileCSV = FileUtils.getFile(FileUtils.getUserDirectoryPath() +"/"+prefix + "_CampaignDetails.csv");

		
		/*
		 *  Read the first emailDirect REST API page and get the total number of pages of campaigns.
		 *  You will then use this to iterate from the first page to the last page of API results
		 */
		String url_AllCampaigns = "https://rest.emaildirect.com/v1/Campaigns/All?PageSize="+pageSize+"&PageNumber=1&ApiKey="+apiKey;

		JSONObject allCampaignsObj = null;
		try {
			allCampaignsObj = APICall.callAPI(url_AllCampaigns);
		} catch (Exception e) {
			if (e.getMessage().equals("maxAttempts")) {
				System.err.println("Could not get the number of pages of campaigns. Exiting.");
				e.printStackTrace();
				System.exit(1);
			}
		}
		int totalPages = allCampaignsObj.getInt("TotalPages");

		
		 // Now that you have the total number of pages of campaigns, iterate through all the pages.
		 // After each line, query the API for the campaign's detail url
		for(int page = 1; page <= totalPages; page++) {
			
			url_AllCampaigns = "https://rest.emaildirect.com/v1/Campaigns/All?PageSize="+pageSize+"&PageNumber="+page+"&ApiKey="+apiKey;
			
			try {
				allCampaignsObj = APICall.callAPI(url_AllCampaigns);
			} catch (Exception e) {
				if (e.getMessage().equals("maxAttempts")) {
					System.err.println("Could not get an entire page of campaign results.");
		    		skipCampaignPageCount++;
		    		continue;
				}
			}
			JSONArray items = allCampaignsObj.getJSONArray("Items"); 

			
			// Iterate through each of the campaigns, querying the campaign's detail url
			// Append the detail to the campaignDetail file
			for (int i = 0; i < items.length(); i++) {
				System.out.println("item " + (i+1) + "/" + items.length() + "\tpage " + page + "/" + totalPages);
				
				
				JSONObject campaignItem = items.getJSONObject(i);
				int campId = campaignItem.getInt("CampaignID");
				
				
				// Execute the URL for the campaign extracted above and write into the csv file
				String campaign_url = "https://rest.emaildirect.com/v1/Campaigns/"+campId+"?ApiKey="+apiKey;
				
				JSONObject campaignObj = null;
				try {
					campaignObj = APICall.callAPI(campaign_url);
				} catch (Exception e) {
					if (e.getMessage().equals("maxAttempts")) {
						System.err.println("Could not get campaign results.");
			    		skipCampaignCount++;
			    		continue;
					}
				}

				

				
				// Skip campaigns that do not have status Sent or Scheduled
				String status = campaignObj.getString("Status");
				if (!(status.equals("Sent") || status.equals("Scheduled"))) {
					System.out.println("Skipping campaign. Status: " + status);
					continue;
				}
				
				
				JSONArray campaignArr = new JSONArray();
				campaignArr.put(campaignObj);
				campaignObj.put("Date_sws", todayDate.toString(datePattern));
				campaignObj.put("ClientName_sws", clientName);
					
				APICall.toCSVFile(campaignArr, campaignDetailsFileCSV, false);
				
	
				// If the scheduled date is > the maxAge parameter, then skip the campaign.
				// If maxAge is set to 0, then don't compare days between -- just pull campaigns from ALL schedDates
				// Do this check after writing campaign detail to file so that it saves data for all campaigns, regardless of date
				LocalDate schedDate = LocalDate.parse(campaignObj.getString("ScheduleDate").substring(0, 10));
				if (maxAge != 0 && Days.daysBetween(schedDate, todayDate).getDays() > maxAge) {
					System.out.println("Skipping campaign. ScheduleDate: " + schedDate.toString(datePattern));
					continue;
				}
					
				
				JSONArray jsonArr = campaignObj.getJSONArray("Links");
				campaignID = campaignObj.getInt("CampaignID");
				campaignIdList.add(campaignID);
				campaignName = cleanCampaignName(campaignObj.getString("Name"));
				
				// Generate a new campaign file for each separate campaign
		    	File emailEventFileCSV = FileUtils.getFile(getCampaignFileName(prefix, campaignID, schedDate));
		    	
		    	// Iterate through all of the info links for each campaign. 
		    	// Append all of the links' info to the same file for each campaign.
				for (int a=0; a < jsonArr.length(); a++) {
			    	JSONObject jcobject = jsonArr.getJSONObject(a);
			    	
			    	// Skip the links that are for "self", "links", and "email"... we don't need them
			    	String rel = jcobject.getString("rel");
			    	if (rel.equals("self") || rel.equals("links") || rel.equals("email"))
			    		continue;
			    	String campHref = jcobject.getString("href");
			    			
			    	
			    	// Determine how many pages of results the campaign href has (Opens, Clicks, Removes, etc)
			    	// Then you can iterate through all the pages of results
			    	String campHref_url = campHref+"?PageSize="+pageSize+"&ApiKey="+apiKey;
			    	
			    	JSONObject campObj = null;
			    	try {
			    		campObj = APICall.callAPI(campHref_url);
			    	} catch (Exception e) {
			    		if (e.getMessage().equals("maxAttempts")) {
							System.err.println("Could not get total pages of event link (opens/clicks/etc) results.");
				    		skipLinkCount++;
				    		continue;
						}
			    	}
			    	
			    	maxPage = APICall.getTotalPages(campObj);
			    	if(maxPage == 0)
		    			continue;
			    	
		    		JSONObject campObject = null;
			    	for(int j = 1; j <= maxPage; j++) {
			    		String campUrl = campHref+"?PageSize="+pageSize+"&PageNumber="+j+"&ApiKey="+apiKey;
			    		
			    		try {
			    			campObject = APICall.callAPI(campUrl);
			    		} catch (Exception e) {
			    			if (e.getMessage().equals("maxAttempts")) {
								System.err.println("Skipping a page of email event.");
					    		skipEventCount++;
					    		continue;
							}
			    		}

			    		JSONArray campArray = campObject.getJSONArray("Items");
			    		addSWSFields_EmailEvents(campArray, todayString, rel, clientName, campaignID, campaignName);
			    		APICall.toCSVFile(campArray, emailEventFileCSV, true);
			    	}					
			   	}
				APICall.uploadToS3(s3client, emailEventDir, emailEventFileCSV);
			}
		}
		APICall.uploadToS3(s3client, emailDetailDir, campaignDetailsFileCSV);

		
		
		
		/***********************************************************************************
		 * Get the link details for all the campaigns scheduled within the given age parameter
		 */
		int skipLinkDetails = 0;
		File linkCSVFile 	= FileUtils.getFile(FileUtils.getUserDirectoryPath() + "/" + prefix + "_Links.csv");
		
		for (int i = 0; i < campaignIdList.size(); i++) {
			String linkURL = "https://rest.emaildirect.com/v1/Campaigns/"+campaignIdList.get(i)+"/Links?PageSize="+pageSize+"&ApiKey="+apiKey;
			
			JSONObject allLinksObj =  null;
			try {
				allLinksObj = APICall.callAPI(linkURL);
			} catch (Exception e) {
				if (e.getMessage().equals("maxAttempts")) {
					System.err.println("Exiting program since we cannot retrieve the list of tracked links.");
					System.exit(1);
				}
			}
			JSONArray links = allLinksObj.getJSONArray("TrackedLinks");
			
			File linkDetailCSVFile 	= FileUtils.getFile(FileUtils.getUserDirectoryPath() + "/" + prefix + "_" + campaignIdList.get(i) + "_LinkDetails.csv");
			
			for (int j = 0; j < links.length(); j++) {
				System.out.println("Link " + (j+1)+"/"+links.length() + "    Campaign " + (i+1) + "/"+campaignIdList.size());
				JSONObject linkChildObj = links.getJSONObject(j);
				int linkID = linkChildObj.getInt("LinkID");
				String linkDetailURL = "https://rest.emaildirect.com/v1/Campaigns/"+campaignIdList.get(i)+"/Links/"+linkID+"/Clicks?PageSize="+pageSize+"&ApiKey="+apiKey;
					
				JSONObject allLinkDetailObj = null;
				try {
					allLinkDetailObj = APICall.callAPI(linkDetailURL);
				} catch (Exception e) {
					if (e.getMessage().equals("maxAttempts")) {
						System.err.println("Skipping this link's details after attempting to query API maxAttempt times.");
						skipLinkDetails++;
						continue; // not the end of the world if you skip a link detail here and there... better to just pick it up
									// on the next run that to have the whole program crash because one API query failed
					}
				}
				JSONArray linkDetail = allLinkDetailObj.getJSONArray("Items");
				
				linkDetail = addSWSFields_LinkDetails(linkDetail, todayString, clientName, campaignIdList.get(i), linkID);
				APICall.toCSVFile(linkDetail, linkDetailCSVFile, true);
			}
			APICall.uploadToS3(s3client, linkDetailDir, linkDetailCSVFile);
			
			links = addSWSFields_Links(links, todayString, clientName, campaignIdList.get(i));
			APICall.toCSVFile(links, linkCSVFile, true);
		}
		APICall.uploadToS3(s3client, linkDir, linkCSVFile);
		
		
		System.err.println("Skipped " + skipCampaignPageCount + " entire pages of campaigns due to errors.");
		System.err.println("Skipped " + skipCampaignCount + " campaigns due to errors.");
		System.err.println("Skipped " + skipLinkCount + " links due to errors.");
		System.err.println("Skipped " + skipEventCount + " email events due to errors.");
		System.err.println("Skipped " + skipLinkDetails + " link details due to errors.");
	}
	
	/**
	 * 
	 * @param prefix
	 * @param campaignID
	 * @return a filename formed from the prefix, campaignID, and date
	 */
	public static String getCampaignFileName(String prefix, int campaignID, LocalDate scheduledDate) {
		return FileUtils.getUserDirectoryPath() +"/"+ prefix + "_" + campaignID + "_" + scheduledDate.toString(datePattern) + ".csv";
	}
	
	
	/**
	 * Scrubs the campaign name, replacing any commas with underscore
	 * @param name
	 */
	public static String cleanCampaignName(String name) {
		return name.replace(",", "_");
	}
	
	/********************************************************************************************
	 * Insert our own fields into a JSONArray -- there is a version of this method for each 
	 * type of output data since they all have different fields to input
	 * @param array JSONArray to insert our fields into at each index
	 * @param date today's date as a string
	 * @param clientName
	 * @param campaignID
	 * @param linkID
	 * @return
	 */
	public static JSONArray addSWSFields_LinkDetails(JSONArray array, String date, String clientName, int campaignID, int linkID) {
		JSONArray arr = array;
		for (int z = 0; z < arr.length(); z++) {
			JSONObject childObject = arr.getJSONObject(z);
			childObject.put("RunDate_sws", date);
			childObject.put("ClientName_sws", clientName);
			childObject.put("CampaignID_sws", campaignID);
			childObject.put("LinkID_sws", linkID);
		}
		return arr;
	}
	/*************************************************************************************************
	 * Insert our own fields into a JSONArray -- there is a version of this method for each 
	 * type of output data since they all have different fields to input
	 * @param array JSONArray to insert fields into at each index
	 * @param date today's date as a string
	 * @param clientName
	 * @param campaignID
	 * @return
	 */
	public static JSONArray addSWSFields_Links(JSONArray array, String date, String clientName, int campaignID) {
		JSONArray arr = array;
		for (int z = 0; z < arr.length(); z++) {
			JSONObject childObject = arr.getJSONObject(z);
			childObject.put("RunDate_sws", date);
			childObject.put("ClientName_sws", clientName);
			childObject.put("CampaignID_sws", campaignID);
		}
		return arr;
	}
	
	/*************************************************************************************************
	 * Insert our own fields into a JSONArray -- there is a version of this method for each 
	 * type of output data since they all have different fields to input
	 * @param array JSONArray to insert fields into at each index
	 * @param date today's date as a string
	 * @param rel the url for the email event
	 * @param clientName
	 * @param campaignID
	 * @param campaignName
	 * @return
	 */
	public static JSONArray addSWSFields_EmailEvents(JSONArray array, String date, String rel, String clientName,
													int campaignID, String campaignName) {
		JSONArray arr = array;
		for (int z = 0; z < arr.length(); z++) {
			JSONObject childObject = arr.getJSONObject(z);
			childObject.put("Date_sws", date);
			childObject.put("Rel_sws", rel);
			childObject.put("ClientName_sws", clientName);
			childObject.put("CampaignID_sws", campaignID);
			childObject.put("CamapignName_sws", campaignName);
		}
		return arr;
	}

}
