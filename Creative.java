package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;

import org.apache.http.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Represents a single Creative taken from the creative database table.
 */
public class Creative {
	
	private String creativeName;
	private String htmlVersion;
	private String textVersion;
	private String htmlS3URL;
	private String htmlS3Filename;
	private String textS3Filename;
	private String textS3URL;
	private JSONArray linkArray;
	private String creativeID;
	private String creativeTimestamp;
	
	private String htmlContent, textContent;
	
	/**
	 * Unpack a query's results into a new Creative object.
	 * This must be created from results from the creative table... anything else will error
	 * 
	 * @param set a database query's results
	 * @throws SQLException, Exception 
	 */
	public Creative(ResultSet set, AmazonS3 s3client) throws SQLException, Exception {
		creativeName = set.getString("creativeName");
		htmlVersion = set.getString("htmlVersion");
		textVersion = set.getString("textVersion");
		htmlS3URL = set.getString("htmlS3URL");
		htmlS3Filename = set.getString("htmlS3Filename");
		textS3URL = set.getString("textS3URL");
		textS3Filename = set.getString("textS3Filename");
		linkArray = new JSONArray(set.getString("linkArray"));
		creativeID = set.getString("creativeID");
		creativeTimestamp = set.getString("creativeTimestamp");
		
		if (htmlS3URL != null && htmlS3Filename != null)
			htmlContent = getS3Content(s3client, htmlS3URL, htmlS3Filename);
		
		if (textS3URL != null && textS3Filename != null)
			textContent = getS3Content(s3client, textS3URL, textS3Filename);
		
		if (htmlContent == null && textContent == null)
			throw new Exception("Need at least one of html or text.");
		
		//System.out.println("HTML Content " + htmlContent);
	}
	
	public String getName() {
		return creativeName;
	}
	
	public String getHtmlVersion() {
		return htmlVersion;
	}
	
	public String getTextVersion() {
		return textVersion;
	}
	
	public String getHtmlS3URL() {
		return htmlS3URL;
	}
	
	public String getTextS3URL() {
		return textS3URL;
	}
	
	public String getLinkArray() {
		return linkArray.toString();
	}
	
	public String getCreativeID() {
		return creativeID;
	}
	
	public String getCreativeTimestamp() {
		return creativeTimestamp;
	}
	
	public void setCreativeID(String id) {
		creativeID = id;
	}
	
	public void setCreativeTimestamp(String time) {
		creativeTimestamp = time;
	}
	
	/**
	 * Get the parameters needed to create a creative on emaildirect. Test if the required
	 * parameters are null. If they are null, throw an error. Else set the parameter. 
	 * @return an ArrayList containing the needed parameters
	 * @throws Exception
	 */
	public StringEntity getParams() throws Exception {
		JSONObject content = new JSONObject();
		content.put("Name", creativeName);
		if (htmlContent != null) {
			System.out.println("Adding html content");
			content.put("HTML", htmlContent);
		}
		if (textContent != null) {
			System.out.println("Adding text content");
			content.put("Text", textContent);
		}
		content.put("TrackLinks", true);
		if (linkArray != null)
			content.put("Links", linkArray);
		
		if (creativeName == null)
			throw new Exception("Cannot use creative with null name.");
		
		return new StringEntity(content.toString(), "UTF-8");
	}
	
	
	
	public static String getS3Content(AmazonS3 s3client, String bucket, String fileName) {
		String content = null;
		try {
			S3Object obj = s3client.getObject(new GetObjectRequest(bucket, fileName));
			
			BufferedReader rd = new BufferedReader(new InputStreamReader(obj.getObjectContent(), "UTF-8"));
			content = rd.lines().collect(Collectors.joining()).toString();
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
			        		   "an internal error while trying to communicate with S3, such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return content;
	}
	
}
