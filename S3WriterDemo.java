package com.scalawagstudio.emailDirect;


import java.io.File;
import java.util.ArrayList;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;


public class S3WriterDemo {
	private static String filename = "JEM_Aurora_Link.csv";
	private static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	public static void main(String[] args) {
		final String localPath 	= args[0];
		final String bucketName = args[1];
		final String destinationDir = bucketName.concat("/ads_ts");
		System.out.println(destinationDir);
		long size;
		String name;
		String s3Path = "ads_ts";
	  	
		//ArrayList<S3FileObjects> list = new ArrayList<S3FileObjects>();
		File file = new File(localPath+"/"+filename);
		double megaBytes = file.length()/(1024*1024);
		if(megaBytes<2.0){
			S3Writer.uploadToS3(s3Client, destinationDir, file);
		}
		S3Writer.uploadMultiPartToS3(s3Client, destinationDir, file);
		//list = S3FileObjects.getObjectList(s3Client, destinationDir, prefix);
		S3Writer.uploadDir(s3Client, destinationDir, localPath, "ads_ts");
	}
}
