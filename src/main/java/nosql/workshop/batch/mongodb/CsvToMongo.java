package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by hadri on 08/02/2016.
 */
public class CsvToMongo {

    public static void main(String[] args) {
        CsvToMongo csvToMongo = new CsvToMongo();
        csvToMongo.activities();
    }

    public void activities() {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection col = db.getCollection("activites");

        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader("src\\main\\resources\\batch\\csv\\activites.csv"));
            List<String[]> myEntries = reader.readAll();
            for (String[] columns : myEntries) {
                System.out.println("ici :" + columns[9]);

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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
