package nosql.workshop.batch.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

public class BatchJob {

    public static final String SEPARATOR = ",";

    public static void main(String[] args) {

        try (MongoClient mongoClient = new MongoClient()) {
            Path path = Paths.get("src/main/resources/batch/csv", "installations.test.csv");
            Reader source = Files.newBufferedReader(path, Charset.forName("UTF-8"));

            MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
            MongoCollection<Document> collection = db.getCollection("installations");

            try (BufferedReader reader = new BufferedReader(source)) {
                reader.lines()
                        .skip(1)
                        .map(line -> Arrays.asList(line.split(SEPARATOR)))
                        .map(line -> line.stream().map(i -> i.replaceAll("\"", "")).collect(Collectors.toList()))
                        .collect(Collectors.toList())
                        .forEach(r -> {
                            Document doc = new Document("_id", r.get(1))
                                    .append("nom", r.get(0))
                                    .append("multiCommune", r.get(17))
                                    .append("nbPlacesParking", r.get(18))
                                    .append("nbPlacesParkingHandicapes", r.get(19))
                                    .append("dateMiseAJourFiche", r.get(29))
                                    .append("adresse",
                                            new Document("numero", r.get(6))
                                                    .append("voie", r.get(7))
                                                    .append("lieuDit", r.get(5))
                                                    .append("codePostal", r.get(4))
                                                    .append("commune", r.get(2)))
                                    .append("location",
                                            new Document("type", r.get(8))
                                                    .append("coordinates", Arrays.asList(r.get(10), r.get(11)) ));


                            collection.insertOne(doc);
                        });




            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            path = Paths.get("src/main/resources/batch/csv", "equipements.test.csv");
            source = Files.newBufferedReader(path, Charset.forName("UTF-8"));

            try (BufferedReader reader = new BufferedReader(source)) {
                reader.lines()
                        .skip(1)
                        .map(line -> Arrays.asList(line.split(SEPARATOR)))
                        .collect(Collectors.toList())
                        .forEach(r -> {
                            Document doc = new Document("numero", r.get(4))
                                    .append("nom", r.get(5))
                                    .append("type", r.get(7))
                                    .append("famille", r.get(9));
                            UpdateResult ur = collection.updateOne(
                                    new Document().append("_id", r.get(2)),
                                    new Document().append("$addToSet",
                                            new Document().append("equipements", doc))
                            );
                            System.out.println("matched=" + ur.getMatchedCount() + " modified="+ ur.getModifiedCount());
                        });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
