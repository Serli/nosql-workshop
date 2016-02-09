package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;

import java.io.*;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

import org.jongo.Jongo;

import nosql.workshop.services.FillDb;
import nosql.workshop.services.MongoDB;

import com.mongodb.*;

/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {
	private static final String INSTALLATIONS = "installations";
	private static final String ACTIVITES = "activites";
	private static final String EQUIPEMENTS = "equipements";
	private static final String VILLES = "towns_paysdelaloire";
	private Jongo connection;


	public CsvToMongoDb(){
		try {
			this.connection = new MongoDB().getJongo();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} 
	}

	private BasicDBObject createObject(String[] columns,String type) {
		switch (type){
		case INSTALLATIONS : return createInstallationObject(columns);
		case ACTIVITES : return createActiviteObject(columns);
		case EQUIPEMENTS : return createEquipementObject(columns);
		case VILLES : return createVilleObjet(columns);

		}
		return null;
	}

	private  BasicDBObject createVilleObjet(String[] columns) {
		return null;
	}

	private BasicDBObject createEquipementObject(String[] columns) {
		BasicDBObject object = new BasicDBObject()
		.append("_id", columns[4])
		.append("nom",columns[5])
		.append("type",columns[7])
		.append("famille",columns[9]);

		connection.getCollection(INSTALLATIONS).update("{_id:"+columns[2]+"}").with("{$push : {"+EQUIPEMENTS+": #}}",object);
		return object;
	}

	private BasicDBObject createActiviteObject(String[] columns) {
		BasicDBObject object =  new BasicDBObject()
		.append("nom",columns[5])
		.append("_id",columns[4]);

		connection.getCollection(EQUIPEMENTS).update("{_id:"+columns[2]+"}").with("{$push : {"+ACTIVITES+": #}}",object);
		return object;

	}

	private BasicDBObject createInstallationObject(String[] columns){
		BasicDBObject object =  new BasicDBObject()
		.append("_id", columns[1])
		.append("nom", columns[0])
		.append("adresse",
				new BasicDBObject()
		.append("numero", columns[6])
		.append("voie", columns[7])
		.append("lieuDit", columns[5])
		.append("codePostal", columns[4])
		.append("commune", columns[2])
				)
				.append(
						"location",
						new BasicDBObject("type", "Point")
						.append("coordinates", Arrays.asList(
								Double.valueOf(columns[10]),
								Double.valueOf(columns[9])
								)
								)
						)
						.append("multiCommune", "Oui".equals(columns[16]))
						.append("nbPlacesParking", columns[17].isEmpty() ? null : Integer.valueOf(columns[17]))
						.append("nbPlacesParkingHandicapes", columns[18].isEmpty() ? null : Integer.valueOf(columns[18]))
						.append(
								"dateMiseAJourFiche",
								columns.length < 29 || columns[28].isEmpty() || columns[28].length()<10
								? null :
									Date.from(
											LocalDate.parse(columns[28].substring(0, 10))
											.atStartOfDay(ZoneId.of("UTC"))
											.toInstant()
											)
								);
		connection.getDatabase().getCollection(INSTALLATIONS).insert(object);
		return object;
	}
	private String getSpliter(String type){
		switch(type){
		case INSTALLATIONS: return "\",\"";
		default: return ",";
		}
	}

	public void extractCsv(String type){ //"/batch/csv/installations.csv"
		String path = "/batch/csv/"+type.toLowerCase()+".csv";
		connection.getDatabase().createCollection(type, null);
		try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream(path);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			reader.lines()
			.skip(1)
			.filter(line -> line.length() > 0)
			.map(line -> line.split(getSpliter(type)))
			.forEach(columns -> {
				createObject(lifting(columns),type);
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	public String[] lifting(String[] array){
		String[] result = new String[array.length];
		for(int i=0; i< array.length; i++){
			result[i] = array[i].matches("\".*\"")?array[i].substring(1,array[i].length()-1):array[i];
		}
		return result;

	}

	public static void main(String[] args) {
		new CsvToMongoDb().fillDB();
	}


	public void fillDB(){
		extractCsv(INSTALLATIONS);
		extractCsv(EQUIPEMENTS);
		extractCsv(ACTIVITES);
	}
}
