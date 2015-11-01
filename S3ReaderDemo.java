package com.scalawagstudio.emailDirect;

import java.io.File;
import java.util.ArrayList;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3ReaderDemo {
	private static String filename = "JEM_Aurora_Link.csv";
	private static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	public static void main(String[] args) {
		final String localPath 	= args[0];
		final String bucketName = args[1];
		final String prefix  = args[2];
		S3Reader.readObjectToLocal(s3Client, bucketName, filename, localPath); // call the reader method
	}
}
