package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;

import java.io.*;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

import org.jongo.Jongo;

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

    private static BasicDBObject createObject(String[] columns,String type) {
        switch (type){
            case INSTALLATIONS : return createInstallationObject(columns);
            case ACTIVITES : return createActiviteObject(columns);
            case EQUIPEMENTS : return createEquipementObject(columns);
            case VILLES : return createVilleObjet(columns);

        }
        return null;
    }

    private static BasicDBObject createVilleObjet(String[] columns) {
        return null;
    }

    private static BasicDBObject createEquipementObject(String[] columns) {
        return null;
    }

    private static BasicDBObject createActiviteObject(String[] columns) {
        return null;
    }

    private static BasicDBObject createInstallationObject(String[] columns){
        return new BasicDBObject()
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
                                                Double.valueOf(columns[9]),
                                                Double.valueOf(columns[10])
                                        )
                                )
                )
                .append("multiCommune", "Oui".equals(columns[16]))
                .append("nbPlacesParking", columns[17].isEmpty() ? null : Integer.valueOf(columns[17]))
                .append("nbPlacesParkingHandicapes", columns[18].isEmpty() ? null : Integer.valueOf(columns[18]))
                .append(
                        "dateMiseAJourFiche",
                        columns.length < 29 || columns[28].isEmpty()
                                ? null :
                                Date.from(
                                        LocalDate.parse(columns[28].substring(0, 10))
                                                .atStartOfDay(ZoneId.of("UTC"))
                                                .toInstant()
                                )
                );
    }
	

    
    public void extractCsv(String type){ //"/batch/csv/installations.csv"
        String path = "/batch/csv/"+type.toLowerCase()+".csv";
    	connection.getDatabase().createCollection(type, null);
    	try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        DBObject object = createObject(columns,type);
                        connection.getDatabase().getCollection(type).insert(object);
                       });
           } catch (IOException e) {
               throw new UncheckedIOException(e);
           }
    }
}
