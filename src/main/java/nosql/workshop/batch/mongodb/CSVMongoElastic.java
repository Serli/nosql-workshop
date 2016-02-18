package nosql.workshop.batch.mongodb;

import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Installation.Adresse;
import nosql.workshop.model.Installation.Location;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CSVMongoElastic {
	DB db;
	DBCollection dbcol;
	JestClient clientElastic;

	public CSVMongoElastic() {
		this.db = new MongoClient().getDB("nosql-workshop");
		this.dbcol = db.getCollection("installations");
		this.clientElastic= ESConnectionUtil.createClient("http://localhost:9200");
	}

	private List<Equipement> getEquipements(BasicDBList equipements) {
		List<Equipement> eqs = new ArrayList<Equipement>();
		for (Object equipement : equipements) {
			BasicDBObject oEq = (BasicDBObject)equipement;
	
			Equipement e = new Equipement();
			e.setNumero(oEq.getString("numero"));
			e.setNom(oEq.getString("nom"));
			e.setType(oEq.getString("type"));
			e.setFamille(oEq.getString("famille"));

			BasicDBList activitesB= (BasicDBList)oEq.get("activites");
			List<String> activites = new ArrayList<String>();
			for(Object act : activitesB) {
				activites.add((String)act);
			}
			e.setActivites(activites);
			eqs.add(e);
		}
		return eqs;
	}
	
	private void saveToMongo() {
		this.run("/batch/csv/activites.csv", "activites");
		this.run("/batch/csv/equipements.csv", "equipements");
		this.run("/batch/csv/installations.csv", "installations");
	}

	private void saveToElastic() {
		try {
			this.clientElastic.execute(new CreateIndex.Builder("installations").build());
			DBCursor cursor = this.dbcol.find();
			while(cursor.hasNext()) {
				BasicDBObject installation = (BasicDBObject)cursor.next();
				Installation source = new Installation();
				source.set_id(installation.getString("_id"));
				source.setNom((String)installation.getString("nom"));
				
				BasicDBObject adresseB = (BasicDBObject)installation.get("adresse");
				Adresse adresse = new Adresse();
				adresse.setNumero(adresseB.getString("numero"));
				adresse.setVoie(adresseB.getString("voie"));
				adresse.setLieuDit(adresseB.getString("lieuDit"));
				adresse.setCodePostal(adresseB.getString("codePostal"));
				adresse.setCommune(adresseB.getString("commune"));
				source.setAdresse(adresse);
				
				BasicDBObject locationB = (BasicDBObject)installation.get("location");
				Location location = new Location();
				location.setType(locationB.getString("type"));
				BasicDBList coordinates = (BasicDBList)locationB.get("coordinates");
				location.setCoordinates(new double[]{(double)coordinates.get(0), (double)coordinates.get(1)});
				source.setLocation(location);
				
				source.setMultiCommune((boolean)installation.get("multiCommune"));
				source.setNbPlacesParking(Integer.parseInt(((String)installation.getString("nbPlacesParking")).length() > 0 ? (String)installation.getString("nbPlacesParking") : "0"));
				source.setNbPlacesParkingHandicapes(Integer.parseInt(((String)installation.getString("nbPlacesParkingHandicapes")).length() > 0 ? (String)installation.getString("nbPlacesParkingHandicapes") : "0"));
				source.setDateMiseAJourFiche((Date)installation.get("dateMiseAJourFiche"));

				source.setEquipements(new ArrayList<Equipement>());
				source.setEquipements(this.getEquipements((BasicDBList)installation.get("equipements")));
				
				Index i= new Index.Builder(source).index("installations").type("installation").build();
				this.clientElastic.execute(i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run(String filename, String name) {
		String splitPattern = name.equals("equipements") ? "," : "\",\"";
		try (InputStream inputStream = CSVMongoElastic.class.getResourceAsStream(filename);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			reader.lines()
			.skip(1)
			.filter(line -> line.length() > 0)
			.map(line -> line.substring(1, line.length() - 1))
			.map(line -> line.split(splitPattern))
			.forEach(col -> {
				if (name.equals("equipements")) this.saveEquipements(col);
				else if (name.equals("installations")) this.saveInstallations(col);
				else this.saveActivites(col);
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		System.out.println("Done " + name);

	}

	private void saveActivites(String[] col) {
		DBObject query = new BasicDBObject("equipements", new BasicDBObject("$elemMatch", new BasicDBObject("numero", col[2].trim())));
		DBObject update = new BasicDBObject("$push", new BasicDBObject("equipements.$.activites", col[5]));
		this.dbcol.findAndModify(query, update);
	}

	private void saveEquipements(String[] col) {
		DBObject equipement = new BasicDBObject()
		.append("numero", col[4])
		.append("nom", col[5])
		.append("type", col[7])
		.append("famille", col[8])
		.append("activites", Arrays.asList());
		DBObject query = new BasicDBObject("_id", col[2]);
		DBObject update = new BasicDBObject("$push", new BasicDBObject("equipements", equipement));
		this.dbcol.findAndModify(query, update);
	}

	private void saveInstallations(String[] col) {
		DBObject adresse = new BasicDBObject("numero", col[6])
				.append("voie", col[7])
				.append("lieuDit", col[5])
				.append("codePostal", col[4])
				.append("commune", col[2]);
		
		DBObject location = new BasicDBObject("type", "Point")
		.append("coordinates", Arrays.asList(Float.parseFloat(col[9]), Float.parseFloat(col[10])));
		
		DBObject installation = new BasicDBObject("_id", col[1])
		.append("nom", col[0])
		.append("adresse", adresse)
		.append("location", location)
		.append("multiCommune", col[16].equals("Oui") ? true : false)
		.append("nbPlacesParking", col[17])
		.append("nbPlacesParkingHandicapes", col[18])
		.append("dateMiseAJourFiche", col.length < 29 || col[28].isEmpty() 
				? null : 
					Date.from(java.time.LocalDate.parse(col[28].substring(0,10))
							.atStartOfDay(ZoneId.of("UTC"))
							.toInstant()))
							.append("equipements", Arrays.asList());
		this.dbcol.insert(installation);
	}
	
	private void importTowns() {
		
	}


	public static void main(String[] args) {
		CSVMongoElastic obj = new CSVMongoElastic();
		//obj.dbcol.drop();
		//obj.dbcol.createIndex(new BasicDBObject("$**", "text"));
		//obj.saveToMongo();
		obj.dbcol.createIndex(new BasicDBObject("location", "2dsphere"));
		obj.saveToElastic();
		//obj.importTowns();
	}

}
