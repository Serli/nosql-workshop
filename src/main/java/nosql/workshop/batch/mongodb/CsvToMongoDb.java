package nosql.workshop.batch.mongodb;

import com.fasterxml.jackson.databind.introspect.BasicClassIntrospector;
import com.mongodb.BasicDBObject;

import java.io.*;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;


import nosql.workshop.connection.ESConnection;


import org.jongo.Jongo;

import nosql.workshop.services.MongoDB;


/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {
	public static final String INSTALLATIONS = "installations";
	public static final String ACTIVITES = "activites";
	public static final String EQUIPEMENTS = "equipements";
	public static final String VILLES = "towns_paysdeloire";
	private Jongo connection;
	private ESConnection esConnection;
	int count=0;


	public CsvToMongoDb(){
		try {
			this.connection = new MongoDB().getJongo();
			this.esConnection = new ESConnection("127.0.0.1","9200");

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
		BasicDBObject object = new BasicDBObject()
				.append("_id",columns[0])
				.append("townName", columns[1])
				.append(
						"location",
						new BasicDBObject("type", "Point")
								.append("coordinates", Arrays.asList(
												Double.valueOf(columns[6]),
												Double.valueOf(columns[7])
										)
								)
				);
		connection.getDatabase().getCollection(VILLES).insert(object);
		return object;
	}

	private BasicDBObject createEquipementObject(String[] columns) {
		BasicDBObject object = new BasicDBObject()
		.append("_id", columns[4])
		.append("nom", columns[5])
		.append("type", columns[7])
		.append("famille", columns[9]);

		BasicDBObject pushDat= new BasicDBObject("$push",new BasicDBObject(EQUIPEMENTS,object));
		connection.getDatabase().getCollection(INSTALLATIONS).findAndModify(new BasicDBObject("_id",columns[2]),pushDat);
	//	connection.getCollection(INSTALLATIONS).update("{_id:" + columns[2] + "}").with("{$push : {"+EQUIPEMENTS+": #}}",object);
		return object;
	}

	private BasicDBObject createActiviteObject(String[] columns) {
		BasicDBObject object =  new BasicDBObject()
		.append("nom", columns[5])
		.append("_id", columns[4]);

		/*BasicDBObject pushDat = new BasicDBObject("$push",new BasicDBObject(EQUIPEMENTS,new BasicDBObject("$push",new BasicDBObject(ACTIVITES,object))));

		connection.getDatabase().getCollection(INSTALLATIONS).findAndModify(new BasicDBObject(EQUIPEMENTS,new BasicDBObject("_id",columns[2])),pushDat);
		//connection.getCollection(EQUIPEMENTS).update("{_id:"+columns[2]+"}").with("{$push : {"+ACTIVITES+": #}}",object);*/
		BasicDBObject searchQuery = new BasicDBObject(
				EQUIPEMENTS,
				new BasicDBObject(
						"$elemMatch",
						new BasicDBObject("_id", columns[2].replace(" ",""))
						)
				);

		BasicDBObject updateQuery = new BasicDBObject(
				"$push",
				new BasicDBObject("equipements.$.activites", columns[5])
		);
		connection.getDatabase().getCollection(INSTALLATIONS).update(searchQuery, updateQuery);
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
								columns.length < 29 || columns[28].isEmpty() || columns[28].length() < 10
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
		System.out.print("Starting extraction for "+type);
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
				if(count==150){
					System.out.print(".");
					count=0;
				}
				else{
					count++;
				}
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		System.out.println("\nDone with : "+type);
	}
	public String[] lifting(String[] array){
		String[] result = new String[array.length];
		for(int i=0; i< array.length; i++){
			result[i] = array[i].matches("\".*\"")?array[i].substring(1,array[i].length()-1):array[i];
		}
		return result;

	}

	public void createIndex(){
		connection.getCollection(INSTALLATIONS).ensureIndex(String.join("",
				"{",
				"*nom*:*text*,",
				"*adresse.commune*:*text*",
				"},",
				"{",
				"*weights*:{",
				"*nom*:3,",
				"*adresse.commune*:10",
				"},",
				"*default_language*:*french*",
				"}"
		).replace('*', '"'));


		connection.getCollection(INSTALLATIONS).ensureIndex("{*location*:*2dsphere*}".replace('*', '"'));
	}

    public void fillDB(){
        extractCsv(INSTALLATIONS);
        extractCsv(EQUIPEMENTS);
        extractCsv(ACTIVITES);
		extractCsv(VILLES);
		createIndex();
		fillElasticSearch(VILLES);
    }

	private void fillElasticSearch(String collection){
		esConnection.insertInIndex(connection.getCollection(collection));
	}


    public static void main(String[] args) {
        new CsvToMongoDb().fillDB();
    }

}
