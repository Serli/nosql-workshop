package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CsvToMongo {
	DB db;
	
	public CsvToMongo() {
		 this.db = new MongoClient().getDB("nosql-workshop");
	}
	
	public void run(String filename, String collection) {

		String csvFile = filename;
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(csvFile));
			line = br.readLine();
			String[] keys = line.split(",");
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				this.saveToMongo(keys, values, collection);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Done");
	}


	private void saveToMongo(String[] keys, String[] values, String collectionName) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < keys.length; i++) {
			map.put(keys[i], values[i]);
		}
		 DBCollection collection = db.getCollection(collectionName);
		 DBObject obj = new BasicDBObject(map);
		 collection.insert(obj);
	}


	public static void main(String[] args) {
		CsvToMongo obj = new CsvToMongo();
		obj.run("", "activities");
	}
}
