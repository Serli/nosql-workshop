package nosql.workshop.batch.mongodb;

import java.io.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {
    public static void main(String[] args) {
    	
    	// Initialisation de la db Mongo
    	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    	DB db = mongoClient.getDB( "mydb" );
    	
    	// Creation d'une collection
    	DBCollection coll = db.getCollection("collectionInstallationsSportives");

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                    	// Edition de la collection
                    	DBObject doc = new BasicDBObject()
                        	.append("_id", columns[1])
                        	.append("nom", columns[0].substring(1))
                        	.append("adresse", 
                        			new BasicDBObject()
                        			.append("numero",columns[6])
                        			.append("voie",columns[7])
                        			.append("lieuDit",columns[5])
                        			.append("codePostal",columns[4])
                        			.append("commune",columns[2])
                        			)
                        	.append("location",
                        			new BasicDBObject("type","Point")
                        			.append("coodinates",Arrays.asList(Double.valueOf(columns[9]),Double.valueOf(columns[10])))
                        			)
                        	.append("multiCommune", "Oui".equals(columns[16]))
                        	.append("nbPlacesParking", columns[17].isEmpty()?null:Integer.valueOf(columns[17]))
                        	.append("nbPlacesParkingHandicapes", columns[18].isEmpty()?null:Integer.valueOf(columns[18]))
                        	.append("dateMiseAJourFiche",
                        			columns.length<29 || columns[28].equals("\"")?null:Date.from(LocalDate.parse(columns[28].substring(0, 10))
                        																				.atStartOfDay(ZoneId.of("UTC"))
                        																				.toInstant()
                        																	  ))
                        	.append("elements",Arrays.asList())																  
                        ;
                    	coll.insert(doc);
                        //System.out.println("Une ligne");
                        //System.out.println(columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0]);
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/equipements.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
               reader.lines()
                       .skip(1)
                       .filter(line -> line.length() > 0)
                       .map(line -> line.split(","))
                       .forEach(columns -> {
                       	// Edition de la collection
                    	   String installationId = columns[2];
                    	   BasicDBObject searchQuery = new BasicDBObject("_id",installationId);
                    	   
                    	   BasicDBObject equipement = new BasicDBObject()
                    	   			.append("numero", columns[4])
                    	   			.append("nom", columns[3])
                    	   			.append("type", columns[7])
                    	   			.append("famille", columns[9])
                    	   			.append("activites", Arrays.asList());
                    	   
                    	   BasicDBObject updateQuery = new BasicDBObject(
                    				"$push",
                    				new BasicDBObject("equipements",equipement));
                    	   
                    	   coll.update(searchQuery,updateQuery);
                           //System.out.println("Une ligne");
                           //System.out.println(columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0]);
                       });
           } catch (IOException e) {
               throw new UncheckedIOException(e);
           }
        
        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/activites.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
               reader.lines()
                       .skip(1)
                       .filter(line -> line.length() > 0)
                       .map(line -> line.split("\",\""))
                       .forEach(columns -> {
                       	// Edition de la collection
                    	   String equipementId = columns[2].trim();
                    	   BasicDBObject searchQuery = new BasicDBObject(
                    	   	"equipements" , 
                    	   	new BasicDBObject( 
                    	   		"$elemMatch",
                    	   		new BasicDBObject("numero",equipementId)));

                    	   String activite = columns[5];
                    	   BasicDBObject updateQuery = new BasicDBObject(
                    	   	"$push",
                    	   	new BasicDBObject("equipements.$.activites",activite));

                    	   
                    	   coll.update(searchQuery,updateQuery);
                           //System.out.println("Une ligne");
                           //System.out.println(columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0]);
                       });
           } catch (IOException e) {
               throw new UncheckedIOException(e);
           }
    }
 
 
}
