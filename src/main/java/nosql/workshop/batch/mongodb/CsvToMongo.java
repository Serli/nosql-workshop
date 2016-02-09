package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CsvToMongo {
	DB db;
	DBCollection collection;

	public CsvToMongo() {
		this.db = new MongoClient().getDB("nosql-workshop");
		this.collection = db.getCollection("installation");

	}

	public void run(String filename, String name) {
		String csvFile = filename;
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(csvFile));
			line = br.readLine();
			String[] keys = line.split(",");
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (name.equals("installations")) this.saveInstallations(keys, values);
				else if (name.equals("equipements")) this.saveEquipements(keys, values);
				else this.saveEquipements(keys, values);
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


	private void saveEquipements(String[] keys, String[] values) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < keys.length; i++) {
			map.put(keys[i], values[i]);
		}
		//this.collection.findAndModify (new BasicDBObject("Code INSEE", map.get("InsNumeroInstall")), )
	}

	private void saveInstallations(String[] keys, String[] values) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < keys.length; i++) {
			map.put(keys[i], values[i]);
		}
		DBObject obj = new BasicDBObject();
		this.collection.insert(obj);
	}


	public static void main(String[] args) {
		CsvToMongo obj = new CsvToMongo();
		obj.run("/batch/csv/installations.csv", "installations");
		obj.run("/batch/csv/equipements.csv", "equipements");
		obj.run("/batch/csv/activites.csv", "activites");
	}
}
