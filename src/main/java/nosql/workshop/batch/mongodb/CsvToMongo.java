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
		try (InputStream inputStream = CsvToMongo.class.getResourceAsStream(filename);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			reader.lines()
			.skip(1)
			.filter(line -> line.length() > 0)
			.map(line -> line.split(","))
			.forEach(columns -> {
				if (name.equals("installations")) this.saveInstallations(columns);
				else if (name.equals("equipements")) this.saveEquipements(columns);
				else if (name.equals("equipements")) this.saveEquipements(columns);
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		System.out.println("Done");

	}


	private void saveEquipements(String[] columns) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < keys.length; i++) {
			map.put(keys[i], values[i]);
		}
		DBObject equipement = new BasicDBObject(map);
		DBObject query = new BasicDBObject("Code INSEE", map.get("InsNumeroInstall"));
		DBObject update = new BasicDBObject("$push", new BasicDBObject("equipements", equipement));
		this.collection.findAndModify(query, update);
	}

	private void saveInstallations(String[] columns) {
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
