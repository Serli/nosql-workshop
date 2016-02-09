package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Arrays;
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

	private void saveActivites(String[] columns) {
		//DBObject activites = new BasicDBObject("activites", columns[]);
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
		DBObject point = new BasicDBObject("type", "Point")
				.append("coordinates", Arrays.asList(columns[9],columns[10]));
		DBObject location = new BasicDBObject("location", point);
		DBObject adresse = new BasicDBObject("numero", columns[6])
				.append("voie", columns[7])
				.append("lieuDit", columns[5])
				.append("codePostal", columns[4])
				.append("commune", columns[2]);
		DBObject equipements = new BasicDBObject("equipements", Arrays.asList());
		DBObject installation = new BasicDBObject("_id", columns[1])
				.append("nom", columns[0])
				.append("adresse", adresse)
				.append("location", location)
				.append("multiCommune", columns[16])
				.append("nbPlacesParking", columns[17])
				.append("nbPlacesParkingHandicapes", columns[18])
				.append("dateMiseAJourFiche", columns[28])
				.append("equipements", equipements);
		this.collection.insert(installation);
	}

	public static void main(String[] args) {
		CsvToMongo obj = new CsvToMongo();
		obj.run("/batch/csv/installations.csv", "installations");
		obj.run("/batch/csv/equipements.csv", "equipements");
		obj.run("/batch/csv/activites.csv", "activites");
	}
}
