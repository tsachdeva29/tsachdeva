package com.scalawagstudio.emailDirect;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReadFileDemo {

	 public static void main(String[] args){

/*
 *  Read a text file from local having two columns and store
 *  both the columns in an arraylist.
 */

		 	String currentLine;
		 	
	        ArrayList<ReadFile> list = new ArrayList<ReadFile>();
	        BufferedReader rd=null;
			try {
				rd = new BufferedReader(new FileReader("/home/sws-default/Desktop/sampleFile.txt"));
				currentLine=null;
				while((currentLine = rd.readLine()) != null) {
			        	String[] split = currentLine.split("\t");
			        	String id = split[0].trim();
				        String name = split[1].trim();
				        list.add(new ReadFile(Integer.valueOf(id), name));
			     }

/*
 *  Print the elements of arraylist
 */

				
				for(ReadFile rf: list){
					System.out.println(rf);
				}
			        	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	    
	}
	
	
}
