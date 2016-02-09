package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.bson.Document;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;

public class CsvToMongoToElastic {
	DB db;
	DBCollection collection;
	Client elasticClient;

	public CsvToMongoToElastic() {
		this.db = new MongoClient().getDB("nosql-workshop");
		this.collection = db.getCollection("installation");
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "myClusterName")
				.put("client.transport.sniff", true)
				.build();
		this.elasticClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("host", 9300));
	}

	private void saveToMongo() {
		this.run("/batch/csv/installations.csv", "installations");
		this.run("/batch/csv/equipements.csv", "equipements");
		this.run("/batch/csv/activites.csv", "activites");
	}

	private void saveToElastic() {
		DBCursor cursor = this.collection.find();
		UpdateRequest updateRequest = new UpdateRequest();
		try {
			while(cursor.hasNext()) {
				updateRequest.index("installations");
				updateRequest.type("installation");
				updateRequest.id("1");
				XContentBuilder a = XContentFactory.jsonBuilder()
		        .startObject()
		            .field("gender", "male")
		        .endObject();
				updateRequest.doc(a);
				this.elasticClient.update(updateRequest).get(a);
				cursor.next();
			}
		} finally {
			cursor.close();
		}





		this.elasticClient.close();
	}

	public void run(String filename, String name) {
		String splitPattern = name.equals("equipements") ? "," : "\",\"";
		try (InputStream inputStream = CsvToMongoToElastic.class.getResourceAsStream(filename);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			reader.lines()
			.skip(1)
			.filter(line -> line.length() > 0)
			.map(line -> line.substring(1, line.length() - 1))
			.map(line -> line.split(splitPattern))
			.forEach(columns -> {
				if (name.equals("installations")) this.saveInstallations(columns);
				else if (name.equals("equipements")) this.saveEquipements(columns);
				else this.saveActivites(columns);
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		System.out.println("Done " + name);

	}

	private void saveActivites(String[] columns) {
		DBObject query = new BasicDBObject("equipements", new BasicDBObject("$elemMatch", new BasicDBObject("numero", columns[2].trim())));
		DBObject update = new BasicDBObject("$push", new BasicDBObject("activites", columns[5]));
		this.collection.findAndModify(query, update);
	}

	private void saveEquipements(String[] columns) {
		DBObject equipement = new BasicDBObject()
				.append("numero", columns[4])
				.append("nom", columns[5])
				.append("type", columns[7])
				.append("famille", columns[8])
				.append("activites", Arrays.asList());
		DBObject query = new BasicDBObject("_id", columns[2]);
		DBObject update = new BasicDBObject("$push", new BasicDBObject("equipements", equipement));
		this.collection.findAndModify(query, update);
	}

	private void saveInstallations(String[] columns) {
		DBObject location = new BasicDBObject("type", "Point")
				.append("coordinates", Arrays.asList(columns[9],columns[10]));
		DBObject adresse = new BasicDBObject("numero", columns[6])
				.append("voie", columns[7])
				.append("lieuDit", columns[5])
				.append("codePostal", columns[4])
				.append("commune", columns[2]);
		DBObject installation = new BasicDBObject("_id", columns[1])
				.append("nom", columns[0])
				.append("adresse", adresse)
				.append("location", location)
				.append("multiCommune", columns[16])
				.append("nbPlacesParking", columns[17])
				.append("nbPlacesParkingHandicapes", columns[18])
				.append("dateMiseAJourFiche", columns[28])
				.append("equipements", Arrays.asList());
		this.collection.insert(installation);
	}

	public static void main(String[] args) {
		CsvToMongoToElastic obj = new CsvToMongoToElastic();
		//obj.collection.drop();
		//obj.saveToMongo();
		obj.saveToElastic();
	}
}
