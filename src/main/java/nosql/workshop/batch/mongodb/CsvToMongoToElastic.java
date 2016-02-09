package nosql.workshop.batch.mongodb;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Update;
import io.searchbox.indices.CreateIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

import org.bson.Document;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.LocalDate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.NodeBuilder;

import com.mongodb.BasicDBList;
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
	JestClient elasticClient;

	public CsvToMongoToElastic() {
		this.db = new MongoClient().getDB("nosql-workshop");
		this.collection = db.getCollection("installations");
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig
				.Builder("http://localhost:9200")
		.multiThreaded(true)
		.build());
		this.elasticClient = factory.getObject();
	}

	private void saveToMongo() {
		this.run("/batch/csv/installations.csv", "installations");
		this.run("/batch/csv/equipements.csv", "equipements");
		this.run("/batch/csv/activites.csv", "activites");
	}

	private void saveToElastic() {
		try {
			this.elasticClient.execute(new CreateIndex.Builder("installations").build());
			DBCursor cursor = this.collection.find();
			while(cursor.hasNext()) {
				/*BasicDBObject installation = (BasicDBObject)cursor.next();
				
				BasicDBObject loc = (BasicDBObject)installation.get("location");
				BasicDBList coo = (BasicDBList)loc.get("coordinates");
				
				String coordinates = XContentFactory.jsonBuilder()
						.startArray()
						.value((Double)coo.get(0))
						.value((Double)coo.get(1))
						.endArray().string();
				String location = XContentFactory.jsonBuilder()
						.startObject()
						.field("coordinates", coordinates)
						.endObject().string();
				String source = XContentFactory.jsonBuilder()
						.startObject()
						.field("nom", installation.get("nom"))
						.field("location", location)
						.endObject().string();
				String doc = XContentFactory.jsonBuilder()
						.startObject()
						.field("script", source)
						.endObject().string();
				this.elasticClient.execute(new Update.Builder(doc).index("installations").type("installation").id((String)installation.get("_id")).build());*/
				Client client = NodeBuilder.nodeBuilder().client(true).node().client();
				client.admin().indices().prepareCreate("installations").execute().actionGet();
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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
		.append("coordinates", Arrays.asList(Float.parseFloat(columns[9]), Float.parseFloat(columns[10])));
		DBObject adresse = new BasicDBObject("numero", columns[6])
		.append("voie", columns[7])
		.append("lieuDit", columns[5])
		.append("codePostal", columns[4])
		.append("commune", columns[2]);
		DBObject installation = new BasicDBObject("_id", columns[1])
		.append("nom", columns[0])
		.append("adresse", adresse)
		.append("location", location)
		.append("multiCommune", columns[16].equals("Oui") ? true : false)
		.append("nbPlacesParking", columns[17])
		.append("nbPlacesParkingHandicapes", columns[18])
		.append("dateMiseAJourFiche", columns.length < 29 || columns[28].isEmpty() 
				? null : 
					Date.from(java.time.LocalDate.parse(columns[28].substring(0,10))
							.atStartOfDay(ZoneId.of("UTC"))
							.toInstant()))
							.append("equipements", Arrays.asList());
		this.collection.insert(installation);
	}


	public static void main(String[] args) {
		CsvToMongoToElastic obj = new CsvToMongoToElastic();
		//obj.collection.drop();
		//obj.collection.createIndex(new BasicDBObject("$**", "text"));
		//obj.saveToMongo();
		//obj.collection.createIndex(new BasicDBObject("location", "2dsphere"));
		obj.saveToElastic();
	}
}
