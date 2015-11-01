package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.mysql.jdbc.PreparedStatement;

public class CreateCampaigns {
	
	private static final InstanceProfileCredentialsProvider cred = new InstanceProfileCredentialsProvider();
	private static final AmazonS3 s3client = new AmazonS3Client(cred);
	
	public static void main(String args[]) {
		final Logger LOGGER = LoggerFactory.getLogger(CreateCampaigns.class);
	    ResultSet rs = null;
		
		try {
			rs = dbConnect();
			System.out.println("Connected");
			while(rs.next()){
				Creative creative = new Creative(rs, s3client);
				System.out.println("Created creative object");
				createCreative(creative);
				System.out.println("Sent creative to emaildirect");
			}
			rs.close();
		} catch(Exception e){
			e.printStackTrace();
		}

		try {
			rs = campaignDBConnect();
			while(rs.next()){
				JSONArray arr = new JSONArray(rs.getString("clientArray"));
				for (int i = 0; i < arr.length(); i++) {
					Campaign campaign = new Campaign(rs, arr.getJSONObject(i).getString("client"));
					createCampaign(campaign);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	/*****************************************************************
	 * 
	 * @return
	 * @throws Exception
	 */
	public static ResultSet dbConnect() throws Exception {
		Connection conn = null;
		Statement statement = null;
		ResultSet resultSet = null;	

		String query = "select * from creative where creativeID is null";

		Class.forName("com.mysql.jdbc.Driver").newInstance();

		conn = DriverManager.getConnection(
				"databse_url",
				"username",
				"password");
		statement = conn.createStatement();	
		resultSet = statement.executeQuery(query);
			
		return resultSet;
	}
	
	
	public static ResultSet campaignDBConnect() throws Exception {
		Connection conn = null;
		Statement statement = null;
		ResultSet resultSet = null;	

		String query = "select * from campaign where campaignID is null";

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		
		conn = DriverManager.getConnection(
				"database_url",
				"username",
				"password");
		statement = conn.createStatement();	
		resultSet = statement.executeQuery(query);
				
		return resultSet;
	}
	
	
	/**************************************************************
	 * 
	 * @param creative
	 * @throws Exception
	 */
	public static void createCreative(Creative creative) throws Exception{
		String jsonString = null;
        CloseableHttpClient client = HttpClientBuilder.create().build();
        	
	 	HttpPost post = new HttpPost("https://rest.emaildirect.com/v1/Creatives");
	 	post.setEntity(creative.getParams()); 
	 	post.addHeader("Content-Type", "application/json");
	 	post.addHeader("ApiKey", "apikey");
    	        	
    	HttpResponse response = client.execute(post);
    	
    	HttpEntity entity = response.getEntity(); 
    	if (entity != null) {
    		BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
    		jsonString = rd.lines().collect(Collectors.joining()).toString();
    		rd.close();
    	}
		
    	JSONObject jObj = new JSONObject(jsonString);
           
        if (response.getStatusLine().getStatusCode() == 201) {
        	creative.setCreativeID(""+jObj.getInt("CreativeID"));
        	creative.setCreativeTimestamp(jObj.getString("Created"));
        	dbUpdate(creative);
        }
        else 
        	System.out.println(jObj.toString());
        
	}
	
	
	public static void createCampaign(Campaign campaign) throws Exception{
		String jsonString = null;
        CloseableHttpClient client = HttpClientBuilder.create().build();
	 	HttpPost post = new HttpPost("https://rest.emaildirect.com/v1/Campaigns");
	 	post.addHeader("Content-Type", "application/json");
	 	post.addHeader("ApiKey", campaign.getApiKey());
	 	post.setEntity(campaign.getParams());
	 	
    	HttpResponse response = client.execute(post);
    	
    	HttpEntity entity = response.getEntity(); 
    	if (entity != null) {
    		BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
    		jsonString = rd.lines().collect(Collectors.joining()).toString();
			rd.close();
    	}
		
    	JSONObject jObj = new JSONObject(jsonString);
       
        if (response.getStatusLine().getStatusCode() == 201) {
        	campaign.setCampaignID(""+jObj.getInt("CampaignID"));
        	campaign.setCampaignTimestamp(jObj.getString("Created"));
        	campaignDBUpdate(campaign);
        }
        else 
        	System.out.println(jObj.toString());
	}
	
	
	/******************************************************************
	 * 
	 * @param creative
	 * @throws Exception
	 */
	public static void dbUpdate(Creative creative) throws Exception {

		Connection conn = null;
		PreparedStatement statement = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

		    conn=DriverManager.getConnection(
		    		"database_url",
					"username",
					"password");
		    
			String query = "update creative set creativeID = ? , "
							+ "creativeTimeStamp = ? "
							+ "where creativeName = ?";
			statement = (PreparedStatement) conn.prepareStatement(query); 
			statement.setString(1, creative.getCreativeID());
			statement.setString(2, creative.getCreativeTimestamp());
			statement.setString(3, creative.getName());
			statement.executeUpdate(); 
		}catch (SQLException e) {	
			e.printStackTrace();
		}finally{
			statement.close();
			conn.close();
		}
	}
	
	
	public static void campaignDBUpdate(Campaign campaign) throws Exception {

		Connection conn = null;
		PreparedStatement statement = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

		    conn=DriverManager.getConnection(
		    		"databse_url",
					"username",
					"password");
		    
			String query = "update campaign set campaignID = ? , "
							+ "campaignTimeStamp = ? "
							+ "where campaignName = ?";
			statement = (PreparedStatement) conn.prepareStatement(query); 
			statement.setString(1, campaign.getCampaignID());
			statement.setString(2, campaign.getCampaignTimestamp());
			statement.setString(3, campaign.getCampaignName());
			statement.executeUpdate(); 
		}catch (SQLException e) {	
			e.printStackTrace();
		}finally{
			statement.close();
			conn.close();
		}
	}
	
}	
