package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class APICall {
    
	private static final int	maxAttempts = 4;

	
	/*************************************************************************
	 * Query an API url and retrieve the JSON response data
	 * @param url the url to query 
	 * @return a JSONObject containing the request's data
	 * @throws Exception 
	 */
	public static JSONObject callAPI(String url) throws Exception {
		HttpGet request = new HttpGet(url);
		BufferedReader rd = null;
		JSONObject obj = null;
		
		for (int attempt = 0; attempt <= maxAttempts; attempt++) {
			try {
				CloseableHttpClient client = HttpClientBuilder.create().build();
				HttpResponse response = client.execute(request);
				rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
				obj = new JSONObject(rd.readLine());
				client.close();
				return obj;
			} catch (Exception e) {
				if (attempt == maxAttempts) {
					System.out.println("Error in parsing url:\n\t" + url);
					System.out.println("The response of the request is:\n" + rd.lines().collect(Collectors.joining()).toString());
		    		e.printStackTrace();
		    		throw new Exception("maxAttempts");
				}
			}
		}
		return obj;
	}
	
	/*****************************************************************************************
	 * Convert a JSONArray to a csv-formatted string, remove the special characters that surround
	 * the self hrefs, and append the string to a file
	 * @param items the JSONArray to convert to a csv-formatted string
	 * @param csvFile the File to append the csv string to
	 * @param removeRel whether or not to remove the leading "[{rel:self... and trailing }]"
	 * @throws IOException
	 */
	public static void toCSVFile(JSONArray items, File csvFile, boolean removeRel) throws IOException{
		if (items.length() == 0) {
			System.out.println("Skipping due to length 0");
			return;
		}
		String csvString = CDL.toString(items.getJSONObject(0).names(), items);
		if (removeRel) {
			csvString = csvString.replace("\"[{rel:self,href:","");
			csvString = csvString.replace("}]\"","");
		}
		
		FileUtils.writeStringToFile(csvFile, csvString, true);
	}
	
	
	
	/***********************************************************************************
	 * get the total number of pages of results from an API response. This will change
	 * depending on the customer, number of results per page, etc.
	 * @param line string with the JSON-formatted data
	 * @return
	 */
	public static int getTotalPages(String line){
		return new JSONObject(line).getInt("TotalPages");
	}
	
	/*************************************************************************************
	 * get the total number of pages of results from an API response. This will change
	 * depending on the customer, number of results per page, etc.
	 * @param obj JSONObject with data inside
	 * @return
	 */
	public static int getTotalPages(JSONObject obj){
		return obj.getInt("TotalPages");
	}
	
	
	/***************************************************************************************
	 * Uploads a given file to a given bucket on Amazon S3, then deletes the local file
	 * @param bucket the location on S3 to upload the file to
	 * @param file the local file to upload to S3
	 */
	public static void uploadToS3(AmazonS3 s3client, String bucket, File file) {
		try {
			if (file.exists()) {
				System.out.println("file exists");
				s3client.putObject(new PutObjectRequest(bucket, file.getName(), file));
				System.out.println("Uploaded " + file.getName() + " to S3.");
				FileUtils.forceDelete(file);
			}
			else{
				System.out.println("file exists");
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
            					"to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(1);
		} catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    		   "an internal error while trying to communicate with S3, such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            System.exit(1);
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}
	
	/**
	 * Scrubs the campaign name, replacing any commas with underscore
	 * @param name
	 */
	public static String cleanCampaignName(String name) {
		return name.replace(",", "_");
	}
}
