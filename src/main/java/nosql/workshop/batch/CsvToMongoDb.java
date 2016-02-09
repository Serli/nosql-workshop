package nosql.workshop.batch;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Scanner;

import static java.util.Arrays.asList;

/**
 * @author Adrian
 */
public class CsvToMongoDb {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient();

        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");


        try {
            Scanner scanner = new Scanner(new File(String.valueOf(CsvToMongoDb.class.getResource("/batch/csv/installations.csv"))));
            List<Document> installations = new ArrayList<>();

            String[] fields;
            Document document = new Document();
            scanner.skip("^$");
            scanner.nextLine();
            while (scanner.hasNextLine()) {
                // Get line
                fields = scanner.nextLine().split(",");

                // Clear the document - more efficient than creating a new instance
                document.clear();
                // We consider the csv file coherent at all times
                document.append("_id", fields[1])
                    .append("nom", fields[0])
                    .append("adresse",
                        new Document().append("numero", fields[6])
                            .append("voie", fields[7])
                            .append("lieuDit", fields[5])
                            .append("codePostal", fields[4])
                            .append("commune", fields[2])
                    ).append("location",
                        new Document().append("type", "point")
                            .append("coordinates", asList(Double.parseDouble(fields[9]), Double.parseDouble(fields[10])))
                    ).append("multiCommune", Boolean.parseBoolean(fields[16]))
                    .append("nbPlacesParking", Integer.parseInt(fields[17]))
                    .append("nbPlacesParkingHandicapes", Integer.parseInt(fields[18]))
                    .append("dateMiseAJourFiche", ISODateTimeFormat.dateParser().parseDateTime(fields[28]))
                    ;
                installations.add(document);
            }

            // Bulk insert the installations
            final MongoCollection<Document> dbCollection = db.getCollection("installations");
            dbCollection.insertMany(installations);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

}
