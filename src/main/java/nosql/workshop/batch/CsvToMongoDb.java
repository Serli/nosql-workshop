package nosql.workshop.batch;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.*;

import static java.util.Arrays.asList;

/**
 * @author Adrian
 * Utilitary class - imports CSV data into MongoDb
 */
public class CsvToMongoDb {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient();

        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        final MongoCollection<Document> dbCollection = db.getCollection("installations");
        Document doc = new Document();

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                            parseDocInstallation(columns, doc);
                            dbCollection.insertOne(doc);
                            // Clear the document - more efficient than creating a new instance
                            doc.clear();
                        }
                    );

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
                        parseDocEquipement(columns, doc);
                        dbCollection.updateOne(
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

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/activites.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(columns -> {
                        dbCollection.updateOne(
                                new Document().append("equipements",
                                        new Document("$elemMatch",
                                                new Document("numero", cleanString(columns[2]))
                                        )
                                ),
                                new Document().append("$addToSet",
                                        new Document().append("equipements.$.activites", cleanString(columns[5]))
                                )
                        );
                        doc.clear();
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Handle a line "installation" and returns a mongo-ready bson document
     * @param line the line to parse
     * @param document the document to fill - we pass it in for performance issues
     */
    private static void parseDocInstallation(String[] line, Document document) {
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
                .append("nbPlacesParking", getIntValue(line[17]))
                .append("nbPlacesParkingHandicapes", getIntValue(line[18]))
        ;
    }

    public static void parseDocEquipement(String[] line, Document document) {
        document.append("numero", line[4])
                .append("nom", line[5])
                .append("type", line[7])
                .append("famille", line[9]);
    }

    /**
     * Removes double quotes from a String.
     * @param toClean the string ot clean
     * @return the cleaned string
     */
    private static String cleanString(String toClean) {
        return toClean.matches("\".*\"") ? toClean.substring(1, toClean.length() - 1).trim() : toClean.trim();
    }

    private static int getIntValue(String toClean) {
        String val = cleanString(toClean);
        return val.isEmpty() ? 0 : Integer.parseInt(val);
    }

}
