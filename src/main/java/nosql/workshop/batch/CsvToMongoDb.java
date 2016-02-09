package nosql.workshop.batch;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Adrian
 */
public class CsvToMongoDb {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient();

        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        final MongoCollection<Document> dbCollection = db.getCollection("installations");

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            Document doc = new Document();
            List<Document> installations = new ArrayList<>();
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .limit(10)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                            handleLineInstallation(columns, doc);
                            dbCollection.insertOne(doc);
                            doc.clear();
                        }
                    );

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    /**
     * Handle a line "installation" and returns a mongo-ready bson document
     * @param line the line to parse
     * @param document the document to fill - we pass it in for performance issues
     */
    private static void handleLineInstallation(String[] line, Document document) {

        // Clear the document - more efficient than creating a new instance
        //document = new Document();
        document.clear();
        // We consider the csv file coherent at all times
        document.append("_id", cleanString(line[1]))
                .append("nom", cleanString(line[0]))
                .append("adresse",
                        new Document().append("numero", cleanString(line[6]))
                                .append("voie", cleanString(line[7]))
                                .append("lieuDit", cleanString(line[5]))
                                .append("codePostal", cleanString(line[4]))
                                .append("commune", cleanString(line[2]))
                ).append("location",
                new Document().append("type", "point")
                        .append("coordinates", asList(Double.parseDouble(cleanString(line[9])), Double.parseDouble(cleanString(line[10]))))
        ).append("multiCommune", "Oui".equals(cleanString(line[16])))
                .append("nbPlacesParking", Integer.parseInt(cleanString(line[17])))
                .append("nbPlacesParkingHandicapes", Integer.parseInt(cleanString(line[18])))
        ;
    }

    /**
     * Removes double quotes from a String.
     * @param toClean the string ot clean
     * @return the cleaned string
     */
    private static String cleanString(String toClean) {
        return toClean.matches("\".*\"") ? toClean.substring(1, toClean.length() - 1) : toClean;
    }

}
