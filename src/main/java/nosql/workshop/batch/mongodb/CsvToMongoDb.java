package nosql.workshop.batch.mongodb;

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
	
	private Jongo connection;
	public CsvToMongoDb(){
		try {
			this.connection = new MongoDB().getJongo();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} 
	}
	
	
    public static void main(String[] args) {  
    }
    
    public void extractInstallations(){
    	connection.getDatabase().createCollection(INSTALLATIONS, null);
    	try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
               reader.lines()
                       .skip(1)
                       .filter(line -> line.length() > 0)
                       .map(line -> line.split(","))
                       .forEach(columns -> {
                       	DBObject object = new BasicDBObject()
                       		.append("_id", columns[1])
                       		.append("nom",columns[0])
                       		.append("adresse",
                       				new BasicDBObject()
                       					.append("numero", columns[6])
                       					.append("voie", columns[7])
                       					.append("lieuDit",columns[5])
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
                       		.append("multiCommune","Oui".equals(columns[16]))
                       		.append("nbPlacesParking",columns[17].isEmpty() ? null : Integer.valueOf(columns[17]))
                       		.append("nbPlacesParkingHandicapes", columns[18].isEmpty() ?  null : Integer.valueOf(columns[18]))
                       		.append(
                       				"dateMiseAJourFiche",
                       				columns.length < 29 || columns[28].isEmpty()
                       						? null :
                       						Date.from(
                       								LocalDate.parse(columns[28].substring(0,10))
                       										.atStartOfDay(ZoneId.of("UTC"))
                       										.toInstant()
                       								)
                       		);
                       	connection.getDatabase().getCollection(INSTALLATIONS).insert(object);
                       });
           } catch (IOException e) {
               throw new UncheckedIOException(e);
           }
    }
}
