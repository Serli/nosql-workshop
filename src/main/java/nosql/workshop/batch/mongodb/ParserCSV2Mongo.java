package nosql.workshop.batch.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;

public class ParserCSV2Mongo {
    public static void main(String[] args) throws UnknownHostException {
        String pathActivite = "/batch/csv/activites.csv";
        String pathEquipement = "/batch/csv/equipements.csv";
        String pathInstallation = "/batch/csv/installations.csv";

        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        db.getCollection("installations").drop();

        try (InputStream inputStream = ParserCSV2Mongo.class.getResourceAsStream(pathInstallation);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        int lastColumn = columns.length - 1;
                        db.getCollection("installations").insertOne(new Document()
                                        .append("_id", getColumnValue(columns[1]))
                                        .append("nom", columns[0].matches("\".*") ? columns[0].substring(1, columns[0].length()) : columns[0])
                                        .append("adresse",
                                                new Document()
                                                        .append("numero", getColumnValue(columns[6]))
                                                        .append("voie", getColumnValue(columns[7]))
                                                        .append("lieuDit", getColumnValue(columns[5]))
                                                        .append("codePostal", getColumnValue(columns[4]))
                                                        .append("commune", getColumnValue(columns[2])))
                                        .append("location",
                                                new Document()
                                                        .append("type", "Point")
                                                        .append("coordinates",
                                                                Arrays.asList(
                                                                        Float.valueOf(getColumnValue(columns[9])),
                                                                        Float.valueOf(getColumnValue(columns[10])))))
                                        .append("multiCommune", "Oui".equals(getColumnValue(columns[16])))
                                        .append("nbPlacesParking", columns[17].matches("\".*\"") ? columns[17].substring(1, columns[17].length() - 1) : columns[17].isEmpty() ? null : Integer.valueOf(getColumnValue(columns[17])))
                                        .append("nbPlacesParkingHandicapes", columns[17].matches("\".*\"") ? columns[18].substring(1, columns[18].length() - 1) : columns[18].isEmpty() ? null : Integer.valueOf(getColumnValue(columns[18])))
                                        .append("dateMiseAJourFiche", columns[28] == null || columns[28].isEmpty() || columns[28].length() <= 9
                                                ? null :
                                                Date.from(
                                                        LocalDate.parse(columns[28].substring(0, 10))
                                                                .atStartOfDay(ZoneId.of("UTC"))
                                                                .toInstant()))
                        );
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (InputStream inputStream = ParserCSV2Mongo.class.getResourceAsStream(pathEquipement);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        db.getCollection("installations").updateOne(
                                new Document("_id", getColumnValue(columns[2])),
                                new Document("$addToSet", new Document("equipements", new Document()
                                        .append("numero", getColumnValue(columns[4]))
                                        .append("nom", getColumnValue(columns[5]))
                                        .append("type", getColumnValue(columns[7]))
                                        .append("famille", getColumnValue(columns[9])))));
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (InputStream inputStream = ParserCSV2Mongo.class.getResourceAsStream(pathActivite);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        db.getCollection("installations").updateOne(
                                new Document()
                                        .append("equipements", new Document("$elemMatch", new Document("numero", columns[2].matches(" .*") ? columns[2].substring(1, columns[2].length()) : columns[2]))),
                                new Document()
                                        .append("$addToSet", new Document().append("equipements.$.activites", getColumnValue(columns[5]))));
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getColumnValue(String column) {
        return column.matches("\".*\"") ? column.substring(1, column.length() - 1) : column;
    }
}