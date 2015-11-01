package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;

public class S3Reader {

/*
 *  Read and save the file from S3 to local
 */

	public static String readObjectToLocal(AmazonS3 s3Client, String bucketName,String fileName, String localPath) {
		String content = null;
		BufferedReader rd = null;
		BufferedWriter out = null;
		try {
			File tempFile= new File(localPath,fileName);
			String fName = tempFile.getName(); //get the filename
			File file = new File(localPath,fName);
			GetObjectRequest request = new GetObjectRequest(bucketName, fileName);
			S3Object object = s3Client.getObject(request);
			S3ObjectInputStream objectContent = object.getObjectContent();
			IOUtils.copy(objectContent, new FileOutputStream(file));
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
