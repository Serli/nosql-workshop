package nosql.workshop.batch;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import nosql.workshop.utils.Utils;
import org.bson.Document;

import java.io.*;

import static java.util.Arrays.asList;

/**
 * @author Adrian
 * Utilitary class - imports CSV data into MongoDb
 */
public class CsvToMongoDb {

    /**
     * Batch CsvToMongoDb - imports installation CSV data into the database.
     * @param args args
     */
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        final MongoCollection<Document> dbCollection = db.getCollection("installations");

        Document doc = new Document();

        // Import installations
        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            reader.lines()
                    .skip(1) // Skip the header line
                    .filter(line -> line.length() > 0) // filter empty lines
                    .map(line -> line.substring(1, line.length() -1)) // first and last characters are double quotes
                    .map(line -> line.split("\",\"")) // each field is separeted by ","
                    .forEach(columns -> { // for each line
                            parseDocInstallation(columns, doc);
                            dbCollection.insertOne(doc);
                            // Clear the document - more efficient than creating a new instance
                            doc.clear();
                        }
                    );

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Import equipements
        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/equipements.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        parseDocEquipement(columns, doc);
                        dbCollection.updateOne( // we add the parsed equipment to its installation
                                new Document().append("_id", columns[2]),
                                new Document().append("$addToSet",
                                        new Document().append("equipements", doc)
                                )
                        );
                        doc.clear();
                    });

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Import activities
        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/activites.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        dbCollection.updateOne( // we add the parsed activity to its equipment
                                new Document().append("equipements",
                                        new Document("$elemMatch",
                                                new Document("numero", Utils.cleanString(columns[2]))
                                        )
                                ),
                                new Document().append("$addToSet",
                                        new Document().append("equipements.$.activites", Utils.cleanString(columns[5]))
                                )
                        );
                        doc.clear();
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Creates a 2DSphere index
        dbCollection.createIndex(new Document("location", "2dsphere"));
    }

    /**
     * Handle a line "installation" and returns a mongo-ready bson document
     * @param line the line to parse
     * @param document the document to fill - we pass it in for performance issues
     */
    private static void parseDocInstallation(String[] line, Document document) {
        // We consider the csv file coherent at all times
        document.append("_id", Utils.cleanString(line[1]))
                .append("nom", Utils.cleanString(line[0]))
                .append("adresse",
                        new Document().append("numero", Utils.cleanString(line[6]))
                                .append("voie", Utils.cleanString(line[7]))
                                .append("lieuDit", Utils.cleanString(line[5]))
                                .append("codePostal", Utils.cleanString(line[4]))
                                .append("commune", Utils.cleanString(line[2]))
                ).append("location",
                new Document().append("type", "Point")
                        .append("coordinates", asList(Double.parseDouble(Utils.cleanString(line[9])), Double.parseDouble(Utils.cleanString(line[10]))))
        ).append("multiCommune", "Oui".equals(Utils.cleanString(line[16])))
                .append("nbPlacesParking", Utils.getIntValue(line[17]))
                .append("nbPlacesParkingHandicapes", Utils.getIntValue(line[18]))
        ;
    }

    /**
     * Handle a line "equipement" and returns a mongo-ready bson document
     * @param line the line to parse
     * @param document the document to fill - we pass it in for performance issues
     */
    public static void parseDocEquipement(String[] line, Document document) {
        document.append("numero", line[4])
                .append("nom", line[5])
                .append("type", line[7])
                .append("famille", line[9]);
    }

}
