package nosql.workshop;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import nosql.workshop.services.MongoDB;
import org.bson.Document;
import org.jongo.Jongo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;

public class ParserCSV2Mongo {
    public static void main(String[] args) throws UnknownHostException {
        String pathActivite = "/batch/csv/activites.csv";
        String pathEquipement = "/batch/csv/equipements.csv";
        String pathInstallation = "/batch/csv/installations.csv";

        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");

        try(InputStream inputStream = ParserCSV2Mongo.class.getResourceAsStream(pathInstallation);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))){
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        int lastColumn = columns.length - 1;
                        db.getCollection("installations").insertOne(new Document()
                                            .append("_id", columns[1].matches("\".*\"") ? columns[1].substring(1, columns[1].length() - 1) : columns[1])
                                            .append("nom", columns[0].matches("\".*") ? columns[0].substring(1, columns[0].length()) : columns[0])
                                            .append("adresse",
                                                    new Document()
                                                            .append("numero", columns[6].matches("\".*\"") ? columns[6].substring(1, columns[6].length() - 1) : columns[6])
                                                            .append("voie", columns[7].matches("\".*\"") ? columns[7].substring(1, columns[7].length() - 1) : columns[7])
                                                            .append("lieuDit", columns[5].matches("\".*\"") ? columns[5].substring(1, columns[5].length() - 1) : columns[5])
                                                            .append("codePostal", columns[4].matches("\".*\"") ? columns[4].substring(1, columns[4].length() - 1) : columns[4])
                                                            .append("commune", columns[2].matches("\".*\"") ? columns[2].substring(1, columns[2].length() - 1) : columns[2]))
                                            .append("location",
                                                    new Document()
                                                            .append("type", "Point")
                                                            .append("coordinates",
                                                                    Arrays.asList(
                                                                            columns[9].matches("\".*\"") ? columns[9].substring(1, columns[9].length() - 1) : columns[9],
                                                                            columns[10].matches("\".*\"") ? columns[10].substring(1, columns[10].length() - 1) : columns[10])))
                                            .append("multiCommune", columns[16].matches("\".*\"") ? columns[16].substring(1, columns[16].length() - 1) : columns[16])
                                            .append("nbrPlacesParking", columns[17].matches("\".*\"") ? columns[17].substring(1, columns[17].length() - 1) : columns[17])
                                            .append("nbrPlacesParkingHandicapes", columns[18].matches("\".*\"") ? columns[18].substring(1, columns[18].length() - 1) : columns[18])
                                    .append("dateMiseAJourFiche", columns[lastColumn].matches(".*\"") ? columns[lastColumn].substring(0, columns[lastColumn].length() - 1) : columns[lastColumn])
                            );
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }


        //.append("equipements",
        //        new Document().append("numero", "").append("nom", "").append("type", "").append("famille", "").append("activites", ""))

    }
}