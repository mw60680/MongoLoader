package com.hermes.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;

public class MainApp {
	
	public static void main(String[] args) {
		String inFilePath = "D:\\mahesh\\ol_dump_editions_2022-03-29.txt";
		String errorRecordsFilePath = "D:\\mahesh\\error-records.txt";
		String connectionString = "mongodb://localhost:27017";
		String databaseName = "gaia";
		String collectionName = "editions";
				
		try {
			// Initialize reader on input file
			File file = new File(inFilePath);
			FileReader reader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(reader);
			
			// Initialize writer on error file
			FileWriter writer = new FileWriter(errorRecordsFilePath);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			
			// Mongo connection
			MongoClient mongoClient = MongoClients.create(connectionString);
			MongoDatabase database = mongoClient.getDatabase(databaseName);
			MongoCollection<Document> collection = database.getCollection(collectionName);

			// Drop existing collection
			collection.drop();
			
			List<InsertOneModel<Document>> docs = new ArrayList<>();
			String line = bufferedReader.readLine();
						
			int count = 0;
			int batch = 100;
			String[] lineItems;
						
			while (line != null) {
				lineItems = line.split("\t");
				System.out.println(lineItems[4]);
				
				count++;

				Document doc = Document.parse(lineItems[4]);
				try {
					collection.insertOne(doc);
					
					docs.add(new InsertOneModel<>(Document.parse(lineItems[4])));
					if (count == batch) {
						collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
						docs.clear();
						count = 0;
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					
					bufferedWriter.write(ex.getMessage());
//					bufferedWriter.write(lineItems[4]);
					docs.forEach(item -> {
						try {
							bufferedWriter.write(item.getDocument().toJson());
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
					bufferedWriter.newLine();
					docs.clear();
					count = 0;
				}
				
				line = bufferedReader.readLine();
			}
			
			bufferedReader.close();
			bufferedWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
