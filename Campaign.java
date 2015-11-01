package com.scalawagstudio.emailDirect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.http.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Represents a single Creative taken from the creative database table.
 */
public class Campaign {
	
	public static final String encodingType = "UTF-8";
	
	private String campaignName;
	private String subject;
	private String fromName;
	private String fromEmail;
	private String toName;
	private String segmentIDArray;
	private String clientArray;
	private String creativeName;
	private String publicationID;
	private String scheduledTime;
	private JSONArray defaultValues;
	private String campaignID;
	private String campaignTimestamp;
	
	private int listID;
	private int creativeID;
	private EmailList emailList;
	
	/**
	 * Unpack a query's results into a new Campaign object.
	 * This must be created from results from the campaign table... anything else will error
	 * 
	 * @param set a database query's results
	 * @throws SQLException
	 */
	public Campaign(ResultSet set, String client) throws SQLException {
		campaignName = set.getString("campaignName");
		subject = set.getString("subject");
		fromName = set.getString("fromName");
		toName = set.getString("toName");
		segmentIDArray = set.getString("segmentIDArray");
		creativeName = set.getString("creativeName");
		publicationID = set.getString("publicationID");
		scheduledTime = set.getString("scheduledTime");
		campaignID = set.getString("campaignID");
		campaignTimestamp = set.getString("campaignTimestamp");
		defaultValues = new JSONArray(set.getString("defaultValues"));
		creativeID = findCreativeID(creativeName);
		
		emailList = new EmailList(campaignName, segmentIDArray, client);
		listID = emailList.getListID();
	}
	
	public String getApiKey() {
		return emailList.getApiKey();
	}
	
	public String getCampaignName() {
		return campaignName;
	}
	
	public String getSubject() {
		return subject;
	}
	
	public String getFromName() {
		return fromName;
	}
	
	public String getFromEmail() {
		return fromEmail;
	}
	
	public String getToName() {
		return toName;
	}
	
	public String getSemgmentIDArray() {
		return segmentIDArray;
	}
	
	public String getClientArray() {
		return clientArray;
	}
	
	public String getCreativeID() {
		return creativeName;
	}
	
	public String getPublicationID() {
		return publicationID;
	}
	
	public String getScheduledTime() {
		return scheduledTime;
	}
	
	public String getCampaignID() {
		return campaignID;
	}
	
	public String getCampaignTimestamp() {
		return campaignTimestamp;
	}
	
	public void setCampaignID(String id) {
		campaignID = id;
	}
	
	public void setCampaignTimestamp(String time) {
		campaignTimestamp = time;
	}
	
	public int findCreativeID(String creativeName) {
		Connection conn = null;
		Statement statement = null;
		ResultSet rs = null;	
		int id = -1;
		
		String query = "SELECT creativeID " + 
						"FROM creative " +
						"WHERE creativeName = '"+creativeName+"'";

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
	
			conn = DriverManager.getConnection(
					"database_url",
					"username",
					"password");
			statement = conn.createStatement();	
			rs = statement.executeQuery(query);
			rs.next();
			id = Integer.parseInt(rs.getString("creativeID"));
		}catch (Exception e) {	
			e.printStackTrace();
		}
		return id;
	}
	
	/**
	 * Get the content parameters needed to create an emaildirect campaign. Test whether the 
	 * required fields are null, if so then throw an error. Else set the corresponding parameter.
	 * @return a StringEntity containing a json object with all the needed data
	 * @throws Exception
	 */
	public StringEntity getParams() throws Exception {
		JSONObject content = new JSONObject();
		content.put("Name", campaignName);
		content.put("CreativeID", creativeID);
		content.put("FromName", fromName);
		content.put("Subject", subject);
		content.put("FromEmail", fromEmail);
		content.put("ToName", toName);
		content.put("PublicationID", publicationID);
		content.put("ListID", listID);
		content.put("DefaultValues", defaultValues);
		content.put("ScheduleTime", scheduledTime);

		if (campaignName == null)
			throw new Exception("Cannot use a campaign with a null name.");
		if (creativeName == null)
			throw new Exception("Must have a non-null creativeName.");
		if (fromName == null)
			throw new Exception("Must have a non-null fromName.");
		if (subject == null)
			throw new Exception("Must have non-null subject");
		if (publicationID == null)
			throw new Exception("Must have a non-null publicationID");
		if (scheduledTime == null)
			throw new Exception("Must have a non-null scheduled timestamp.");
		
		return new StringEntity(content.toString(), "UTF-8");
	}
}
