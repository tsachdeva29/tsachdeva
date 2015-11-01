package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mysql.jdbc.PreparedStatement;

public class CampaignData {
	public static int count 	= 0;
	
	/**
	 * Unpack a query's results into a new Creative object.
	 * This must be created from results from the creative table... anything else will error
	 * 
	 * @param set a database query's results
	 * @throws SQLException, Exception 
	 */
	
	public static ResultSet dbConnect() throws Exception {
		Connection conn = null;
		Statement statement = null;
		ResultSet resultSet = null;	

		String query = "select * from creative where creativeID is null";

		try {
				Class.forName("com.mysql.jdbc.Driver").newInstance();

								conn=DriverManager.getConnection("database_url",
						"username","password");
				statement = conn.createStatement();	
				resultSet = statement.executeQuery(query);
				
		}catch (SQLException e) {	
			e.printStackTrace();
		}
		return resultSet;
	}
	
	

	public static void createCreative(Creative creative) throws Exception{
		int status = 0;
		String jsonString = null;
        CloseableHttpClient client = HttpClientBuilder.create().build();
        	
		 	HttpPost post = new HttpPost("https://rest.emaildirect.com/v1/Creatives?ApiKey=apikey");
		 	List<NameValuePair> params = creative.getParams();
        	        	
        	post.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        	HttpResponse response = client.execute(post);
        	
        	HttpEntity entity = response.getEntity(); 
        	if (entity != null) {
        		BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
        		
        		try {
        			   jsonString = rd.lines().collect(Collectors.joining()).toString();
        		       System.out.println("JSON ->"+ jsonString);
        		   } finally {
        		       rd.close();
        		   }
        	}
    		
        	JSONObject jObj = new JSONObject(jsonString);
           
            if(status == 201) {
                 creative.setCreativeID(jObj.getString("CreativeID"));
                 creative.setCreativeTimestamp(jObj.getString("Created"));
                 dbUpdate(creative);
            }
	}
	
	public static void dbUpdate(Creative creative) throws Exception {

		Connection conn = null;
		PreparedStatement statement = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			    
		    conn=DriverManager.getConnection("database_url",
					"username","password");
		    
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
}	