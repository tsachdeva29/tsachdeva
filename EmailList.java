package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

public class EmailList {

	private static final String dbURL = "database_url";
	private static final String username = "username";
	private static final String password = "password";
	
	private JSONArray segments;
	private String client;
	private String name;
	private String apiKey;
	private JSONArray emails;
	private int listID;
	
	public EmailList(String campaignName, String segmentsArray, String clientName) {
		segments = new JSONArray(segmentsArray);
		client = clientName;
		name = campaignName;
		apiKey = findApiKey();
		setEmails();
		createList();
		if ((Integer)listID != null)
			addEmails();
	}
	
	public String findApiKey() {
		Connection conn = null;
		Statement statement = null;
		ResultSet rs = null;	
		String key = null;
		
		String query = "SELECT apiKey " + 
						"FROM emailClients " +
						"WHERE emailClient = '"+client+"'";

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
	
			conn = DriverManager.getConnection(
					"database_url",
					"username",
					"password");
			statement = conn.createStatement();	
			rs = statement.executeQuery(query);
			rs.next();
			key = rs.getString("apiKey");
		}catch (Exception e) {	
			e.printStackTrace();
		}
		return key;
	}
	
	public void setEmails() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet set = null;
		emails = new JSONArray();
		
		try {
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			Properties props = new Properties();
			props.setProperty("user", username);
			props.setProperty("password", password);
			conn = DriverManager.getConnection(dbURL, props);
			stmt = conn.createStatement();

			for (int i = 0; i < segments.length(); i++) {
				String sql = "SELECT email_address " +
						"FROM testseg " +   		// TODO: segmentPriority
						"WHERE segmentID = '"+segments.getJSONObject(i).getString("segmentID")+"' " +
						"AND master_client = '"+client+"'";
			
				set = stmt.executeQuery(sql);
				while (set.next())
					emails.put(set.getString("email_address"));	
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
				if (stmt != null)
					stmt.close();
				if (set != null)
					set.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void createList() {
		BufferedReader rd = null;
		CloseableHttpClient client = HttpClientBuilder.create().build();;
		try {
			HttpPost post = new HttpPost("https://rest.emaildirect.com/v1/Lists");
			post.addHeader("Content-Type", "application/json");
			post.addHeader("ApiKey", apiKey);
			
			JSONObject content = new JSONObject();
			content.put("Name", name);
			
			post.setEntity(new StringEntity(content.toString(), "UTF-8"));
			
			HttpResponse response = client.execute(post);
			HttpEntity entity = response.getEntity();
			
			if (entity != null) {
				rd = new BufferedReader(new InputStreamReader(entity.getContent()));
				JSONObject obj = new JSONObject(rd.lines().collect(Collectors.joining()).toString());
				listID = obj.getInt("ListID");
			}
				
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rd != null)
					rd.close();
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void addEmails() {
		CloseableHttpClient client = HttpClientBuilder.create().build();
		try {
			HttpPost post = new HttpPost("https://rest.emaildirect.com/v1/Lists/"+listID+"/AddEmails");
			
			JSONObject content = new JSONObject();
			content.put("EmailAddresses", emails);
			
			post.addHeader("Content-Type", "application/json");
			post.addHeader("ApiKey", apiKey);
			post.setEntity(new StringEntity(content.toString(), "UTF-8"));
			
			HttpResponse response = client.execute(post);
			
			if (response.getStatusLine().getStatusCode() != 200) {
				System.out.println("ListID " + listID + " response: " + response.getStatusLine());
				System.out.println(new BufferedReader(
						new InputStreamReader(response.getEntity().getContent()))
						.lines().collect(Collectors.joining()).toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (Exception e) {
				
			}
		}
	}
	
	public JSONArray getEmails() {
		return emails;
	}
	
	public String getEmailsString() {
		return emails.toString();
	}
	
	public int getListID() {
		return listID;
	}
	
	public String getApiKey() {
		return apiKey;
	}
}
