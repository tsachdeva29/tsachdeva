package com.scalawagstudio.emailDirect;

import java.util.ArrayList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3FileObjects {
	long size;
	String name;
	public void setSize(long size){
		this.size = size;
	}
	public void setName(String name){
		this.name = name;
	}
	public long getSize(){
		return size;
	}
	public String getName(){
		return name;
	}
	
	public static ArrayList<S3FileObjects> getObjectList(AmazonS3 s3Client, String bucketName, String prefix){
		ArrayList<S3FileObjects> arrList  = new ArrayList<S3FileObjects>();
		try{
			ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest()
				.withBucketName(bucketName)
				.withPrefix(prefix));
			for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				S3FileObjects s = new S3FileObjects();
				s.setSize(objectSummary.getSize());
				s.setName(objectSummary.getKey());
				arrList.add(s);
		    }
		}catch (AmazonServiceException ase) {
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
        
		return arrList;
    }
}
