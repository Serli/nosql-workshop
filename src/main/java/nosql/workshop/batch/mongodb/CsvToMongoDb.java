package nosql.workshop.batch.mongodb;

import com.mongodb.*;

import java.io.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by chriswoodrow on 09/02/2016.
 */

public class CsvToMongoDb {
    public static void main(String[] args) {

        createInstallations();
        createEquipements();
        createActivites();
    }

    public static void createInstallations() {
        String givenUri = System.getenv("MONGOLAB_URI");
        String uri = givenUri == null ? "mongodb://localhost:27017/nosql-workshop" : givenUri;
        MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        DB db = mongoClient.getDB(mongoClientURI.getDatabase());
        DBCollection col = db.getCollection("installations");
        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        System.out.println(columns[28]);
                        String nom = columns[0].replaceAll("[\\\"]",""); //enlever les caractères spéciaux en trop dans le nom
                        DBObject dbo = new BasicDBObject("_id", columns[1])
                                .append("nom", nom)
                                .append("adresse",
                                        new BasicDBObject("numero", columns[6])
                                                .append("voie", columns[7])
                                                .append("lieuDit", columns[5])
                                                .append("codePostal", columns[4])
                                                .append("commune", columns[3])
                                )
                                .append("location",
                                        new BasicDBObject("type", "Point")
                                                .append("coordinates",
                                                        Arrays.asList(Double.valueOf(columns[9]), Double.valueOf(columns[10]))))
                                .append("multiCommune", "Oui".equals(columns[16]))
                                .append("nbPlacesParking", columns[17])
                                .append("nbPlacesParkingHandicapes", columns[18])
                                .append(
                                        "dateMiseAJourFiche",
                                        columns[28] == null || columns[28].isEmpty() || columns[28].length() <= 9
                                                ? null :
                                                Date.from(
                                                        LocalDate.parse(columns[28].substring(0, 10))
                                                                .atStartOfDay(ZoneId.of("UTC"))
                                                                .toInstant()
                                                )
                                );

                        col.save(dbo);
                    });

            System.out.println("fini installations");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void createEquipements() {
        String givenUri = System.getenv("MONGOLAB_URI");
        String uri = givenUri == null ? "mongodb://localhost:27017/nosql-workshop" : givenUri;
        MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        DB db = mongoClient.getDB(mongoClientURI.getDatabase());
        DBCollection col = db.getCollection("installations");

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/equipements.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        col.update(col.findOne(columns[2].trim()),
                                new BasicDBObject("$push",
                                        new BasicDBObject("equipements",
                                            new BasicDBObject("numero", columns[4])
                                            .append("nom", columns[5])
                                            .append("type", columns[7])
                                            .append("famille", columns[9]))));
                    });

            System.out.println("fini equipements");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void createActivites() {
        String givenUri = System.getenv("MONGOLAB_URI");
        String uri = givenUri == null ? "mongodb://localhost:27017/nosql-workshop" : givenUri;
        MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        DB db = mongoClient.getDB(mongoClientURI.getDatabase());
        DBCollection col = db.getCollection("installations");

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/activites.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {

                        String equipementId = columns[2].trim();
                        BasicDBObject searchQuery = new BasicDBObject("equipements",
                                new BasicDBObject("$elemMatch",
                                        new BasicDBObject("numero", equipementId)));
                        String activite = columns[5];
                        BasicDBObject updateQuery = new BasicDBObject(
                                "$push",
                                new BasicDBObject("equipements.$.activites", activite)
                        );
                        col.update(searchQuery, updateQuery);
                    });

            System.out.println("fini activites");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
