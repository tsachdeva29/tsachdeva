package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class BeaconAPI {

private static final int	maxAttempts = 4;

	
	/*************************************************************************
	 * Query an API url and retrieve the JSON response data
	 * @param url the url to query 
	 * @return a JSONObject containing the request's data
	 * @throws Exception 
	 */
	public static JSONObject callAPI(String url, String accept, String apiKey) throws Exception {
		BufferedReader rd = null;
		JSONObject obj = null;
		CloseableHttpClient httpClient = null;
        HttpPost httpost = null;
        CloseableHttpResponse response = null;
		
		for (int attempt = 0; attempt <= maxAttempts; attempt++) {
			try {
				httpClient = HttpClients.createDefault();
	    		HttpGet getRequest = new HttpGet(url);
	    		getRequest.addHeader("Accept", accept);
	    		getRequest.addHeader("Api-Key", apiKey);
	    		response = httpClient.execute(getRequest);
				
				if (response.getStatusLine().getStatusCode() != 200) {
		                throw new RuntimeException("Failed : HTTP error code : "
		                        + response.getStatusLine().getStatusCode());
		            }

		            rd = new BufferedReader(new InputStreamReader(
		                    (response.getEntity().getContent())));
		            obj = new JSONObject(rd.readLine());
		            
		        }catch (MalformedURLException e) {
		            e.printStackTrace();
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
	 * @throws IOException
	 */
	public static void toCSVFile(JSONArray clients, File csvFile) throws IOException{
		if (clients.length() == 0) {
			System.out.println("Skipping due to length 0");
			return;
		}
		String csvString = CDL.toString(clients.getJSONObject(0).names(), clients);
		FileUtils.writeStringToFile(csvFile, csvString, true);
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
				System.out.println("file does not exist");
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
	
}
