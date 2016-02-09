package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by hadri on 08/02/2016.
 */
public class CsvToMongo {

    private MongoClient mongoClient;

    public CsvToMongo() {
        mongoClient = new MongoClient();
    }


    public static void main(String[] args) {
        CsvToMongo csvToMongo = new CsvToMongo();
        csvToMongo.dropDB();
        csvToMongo.installations();
    }

    private void dropDB() {
        DB db = mongoClient.getDB("nosql-workshop");
        db.dropDatabase();
    }

    public void installations() {
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection col = db.getCollection("installation");
        List<String[]> myEntries = readCSV("src\\main\\resources\\batch\\csv\\installations.csv");
        myEntries.stream()
                .skip(1)
                .forEach(columns -> {
                    DBObject obj = new BasicDBObject()
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
                                            .append(
                                                    "coordinates",
                                                    Arrays.asList(
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
                    col.insert(obj);
                });
    }

    public void equipements() {

    }

    public static List<String[]> readCSV(String path) {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(path));
            return reader.readAll();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }


    public void activities() {

        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection col = db.getCollection("activites");
        List<String[]> myEntries = readCSV("src\\main\\resources\\batch\\csv\\activites.csv");


            for (String[] columns : myEntries) {
                System.out.println("ici :" + columns[9]);

                DBObject obj = new BasicDBObject()
                        .append("numero", columns[4])
                        .append("nom", columns[5]);



             /*   DBObject obj = new BasicDBObject()
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
                                        .append(
                                                "coordinates",
                                                Arrays.asList(
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
                        );*/


            }

    }

}
