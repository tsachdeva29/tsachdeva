package com.scalawagstudio.emailDirect;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3Writer {
	
/*
 *  Upload a file from local to S3
 */
	
	public static void uploadToS3(AmazonS3 s3Client, String bucketName, File filename) {
		System.out.println(filename.getName());
		try {
			if (filename.exists()) {
				System.out.println("file exists");
				System.out.println(filename.getName());
				s3Client.putObject(new PutObjectRequest(bucketName, filename.getName(), filename));
				System.out.println("Uploaded " + filename.getName() + " to S3.");
				//FileUtils.forceDelete(file);
			}
			else{
				System.out.println("file does not exists");
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

/*
 *  Upload the file in multiparts if the size of file is hige.
 */

	
	public static void uploadMultiPartToS3(AmazonS3 s3Client, String bucketName, File filename) {
		try{
			 TransferManager tm = new TransferManager(s3Client);
			 Upload upload = tm.upload(bucketName, filename.getName(), filename);
			 upload.waitForCompletion();
		} catch(AmazonClientException amazonClientException) {
	        	System.out.println("Unable to upload file, upload was aborted.");
	        	amazonClientException.printStackTrace();
	      } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

/*
 *  Upload all the files in a local folder to S3
 */

	public static void uploadDir(AmazonS3 s3Client, String bucketName, String  dir, String destinationDir) {
		try{
			 File[] files = new File(dir).listFiles();

			for (File i : files) {
			    System.out.println(i);
			    String key = i.getAbsolutePath().substring(new File(dir).getAbsolutePath().length() + 1).replaceAll("\\\\", "/");
				s3Client.putObject(bucketName, key, i);

			}
			 
		} catch(AmazonClientException amazonClientException) {
        	System.out.println("Unable to upload file, upload was aborted.");
        	amazonClientException.printStackTrace();
		} 	
	}
}