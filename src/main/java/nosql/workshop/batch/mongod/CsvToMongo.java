package nosql.workshop.batch.mongod;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class CsvToMongo {

	public static void main(String[] args) {

		CsvToMongo obj = new CsvToMongo();
		System.out.println("ok1");
		obj.runInstallations();
		System.out.println("ok2");
		obj.runEquipements();
		System.out.println("ok3");
		obj.runActivites();
		System.out.println("ok4");

	}

	public void runActivites() {
		try (InputStream inputStream = CsvToMongo.class.getResourceAsStream("/batch/csv/activites.csv");
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			MongoClient mongo = new MongoClient();
			DB db = mongo.getDB("nosql-workshop");
			DBCollection collection = db.getCollection("installations");
			String line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\",\"");
				//DBObject activite = new BasicDBObject("activite", );
				DBObject query = new BasicDBObject("equipements", new BasicDBObject("$elemMatch", new BasicDBObject("numero", values[2].trim())));
				collection.findAndModify(query, new BasicDBObject("$push", new BasicDBObject("equipements.$.activites", values[5])));
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public void runEquipements() {
		try (InputStream inputStream = CsvToMongo.class.getResourceAsStream("/batch/csv/equipements.csv");
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			MongoClient mongo = new MongoClient();
			DB db = mongo.getDB("nosql-workshop");
			DBCollection collection = db.getCollection("installations");
			String line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				String[] values = line.split(",");
				DBObject equipement = new BasicDBObject("numero", values[4])
						.append("nom", values[5])
						.append("type", values[7])
						.append("famille", values[9])
						.append("activites", Arrays.asList());
				DBObject id = new BasicDBObject("_id", values[2]);
				collection.findAndModify(id,  new BasicDBObject("$push", new BasicDBObject("equipements", equipement)));
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public void runInstallations() {
		try (	InputStream inputStream = CsvToMongo.class.getResourceAsStream("/batch/csv/installations.csv");
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			MongoClient mongo = new MongoClient();
			DB db = mongo.getDB("nosql-workshop");
			DBCollection collection = db.getCollection("installations");
			collection.drop();
			String line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\",\"");
				DBObject adresse = new BasicDBObject("numero", values[6])
						.append("voie", values[7])
						.append("lieuDit", values[5])
						.append("codePostal", values[4])
						.append("commune", values[2]);
				DBObject location = new BasicDBObject("type", "Point")
						.append("coordinates", Arrays.asList(
								Double.valueOf(values[9]),
								Double.valueOf(values[10])
								));
				DBObject document = new BasicDBObject("_id", values[1])
						.append("nom", values[0].substring(1, values[0].length()-1))
						.append("adresse", adresse)
						.append("location", location)
						.append("multiCommune", "Oui".equals(values[16]))
						.append("nbPlacesParking", values[17].isEmpty() ? null : Integer.valueOf(values[17]))
						.append("nbPlacesParkingHandicapes", values[18].isEmpty() ? null : Integer.valueOf(values[18]))
						.append("dateMiseAJourFiche", values.length < 29 || values[28].isEmpty() || values[28].length() < 10 
								? null : 
									Date.from(java.time.LocalDate.parse(values[28].substring(0,10))
									.atStartOfDay(ZoneId.of("UTC"))
									.toInstant()))
						.append("equipements", Arrays.asList());
				collection.insert(document);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

	}
}


