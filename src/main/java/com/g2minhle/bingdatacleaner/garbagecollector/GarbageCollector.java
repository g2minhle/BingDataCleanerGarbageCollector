package com.g2minhle.bingdatacleaner.garbagecollector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.joda.time.DateTime;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

public class GarbageCollector implements RequestHandler<Object, String> {

	static final String GOOGLE_CLIENT_SECRET_ENV_VAR = "GOOGLE_CLIENT_SECRET";
	static final String DYNAMO_DB_TABLE_ENV_VAR = "DYNAMO_DB_TABLE";
	
	static DateTime currentTime = new org.joda.time.DateTime(new Date());

	static Drive DriveServices = null;
	static BatchRequest CleanRequests;
	static List<String> DeleteFileQueue = new LinkedList<String>();
	static JsonBatchCallback<Void> DoNothingCallBack = new JsonBatchCallback<Void>() {
		@Override
		public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders)
				throws IOException {
		}

		public void onSuccess(Void file, HttpHeaders responseHeaders) throws IOException {

		}
	};

	static JsonBatchCallback<File> QueueForDeleteCallBack =
			new JsonBatchCallback<File>() {
				@Override
				public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders)
						throws IOException {
					// Handle error
					System.out.println(e.getMessage());
				}

				public void onSuccess(File file, HttpHeaders responseHeaders)
						throws IOException {
					DateTime fileCreationTime =
							new DateTime(file.getCreatedTime().getValue());

					if (currentTime.getDayOfYear()
							- fileCreationTime.getDayOfYear() > 7) {
						DriveServices.files().delete(file.getId())
								.queue(CleanRequests, DoNothingCallBack);
					}

				}
			};

	private void initServices() {
		List<String> GoogleAPIScopes = new ArrayList<String>();

		GoogleAPIScopes.addAll(DriveScopes.all());

		/** Global instance of the HTTP transport. */
		HttpTransport HTTP_TRANSPORT;
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

		try {
			HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		} catch (GeneralSecurityException t) {
			// TODO add alert
			t.printStackTrace();
			return;
		} catch (IOException e) {
			// TODO Add alert
			e.printStackTrace();
			return;
		}
		String clientSecret = System.getenv(GOOGLE_CLIENT_SECRET_ENV_VAR);
		InputStream in = new ByteArrayInputStream(clientSecret.getBytes());
		
		GoogleCredential credential;
		try {
			credential = GoogleCredential.fromStream(in).createScoped(GoogleAPIScopes);
		} catch (Exception e) {
			// TODO add alert
			e.printStackTrace();
			return;
		}

		DriveServices =
				new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
						.setApplicationName("BingDataCleaner").build();

	}

	private List<File> getFileListing() throws IOException {
		List<File> files = new ArrayList<File>();
		Files.List getListFileRequest = DriveServices.files().list();

		do {
			try {
				FileList fileList = getListFileRequest.execute();

				files.addAll(fileList.getFiles());
				getListFileRequest.setPageToken(fileList.getNextPageToken());
			} catch (IOException e) {
				System.out.println("An error occurred: " + e);
				getListFileRequest.setPageToken(null);
			}
		} while (getListFileRequest.getPageToken() != null
				&& getListFileRequest.getPageToken().length() > 0);

		return files;

	}

	private void cleanOldSheets() throws IOException {
		initServices();
		List<File> files = getFileListing();
		BatchRequest getFileCreationDateRequests = DriveServices.batch();
		CleanRequests = DriveServices.batch();

		for (File file : files) {
			System.out.println(file.getId());
			DriveServices.files().get(file.getId()).setFields("createdTime,id")
					.queue(getFileCreationDateRequests, QueueForDeleteCallBack);
		}
		getFileCreationDateRequests.execute();
		CleanRequests.execute();		
	}

	private void cleanOldDBEntry() {
		DynamoDB DynamoDBInstance = new DynamoDB(new AmazonDynamoDBClient());
		Table Table = DynamoDBInstance.getTable(System.getenv(DYNAMO_DB_TABLE_ENV_VAR));

		ItemCollection<ScanOutcome> scanResults = Table.scan();
		List<PrimaryKey> keysToDelete = new LinkedList<PrimaryKey>();
		for (Item item : scanResults) {
			DateTime entryCreationTime = new DateTime(item.getLong("createdTime"));
			if (currentTime.getDayOfYear() - entryCreationTime.getDayOfYear() > 7) {
				keysToDelete.add(new PrimaryKey("id", item.getString("id")));
			}
		}
		if (keysToDelete.size() == 0)
			return;
		PrimaryKey[] keysToDeleteArray =
				keysToDelete.toArray(new PrimaryKey[keysToDelete.size()]);
		TableWriteItems forumTableWriteItems =
				new TableWriteItems(Table.getTableName())
						.withPrimaryKeysToDelete(keysToDeleteArray);
		BatchWriteItemOutcome outcome =
				DynamoDBInstance.batchWriteItem(forumTableWriteItems);

	}

	public String handleRequest(Object input, Context context) {
		try {
			cleanOldSheets();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cleanOldDBEntry();
		return "";
	}
	
	public static void main(String[] args) throws IOException {
		GarbageCollector garbageCollector = new GarbageCollector();
		try {
			garbageCollector.cleanOldSheets();
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		garbageCollector.cleanOldDBEntry();
	}

}
