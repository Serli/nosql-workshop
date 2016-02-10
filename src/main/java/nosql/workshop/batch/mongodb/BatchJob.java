package nosql.workshop.batch.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class BatchJob {

    public static final String SEPARATOR = ",";

    public static void main(String[] args) {

        try (MongoClient mongoClient = new MongoClient()) {
            Path path = Paths.get("src/main/resources/batch/csv", "installations.csv");
            Reader source = Files.newBufferedReader(path, Charset.forName("UTF-8"));

            MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
            MongoCollection<Document> collection = db.getCollection("installations");

            try (BufferedReader reader = new BufferedReader(source)) {
                reader.lines()
                        .skip(1)
                        .map(line -> Arrays.asList(line.split("\",\"")))
                        .map(line -> line.stream().map(i -> i.replaceAll("\"", "")).collect(Collectors.toList()))
                        .collect(Collectors.toList())
                        .forEach(r -> {
                            Document doc = new Document("_id", r.get(1))
                                    .append("nom", r.get(0))
                                    .append("multiCommune", "Oui".equalsIgnoreCase(r.get(16)))
                                    .append("nbPlacesParking", r.get(17).isEmpty() ? null : Integer.valueOf(r.get(17)))
                                    .append("nbPlacesParkingHandicapes", r.get(18).isEmpty() ? null : Integer.valueOf(r.get(18)))
                                    .append("dateMiseAJourFiche",
                                            r.get(28) == null || r.get(28).isEmpty() || r.get(28).length() <= 9
                                                    ? null :
                                                    Date.from(LocalDate.parse(r.get(28).substring(0, 10))
                                                            .atStartOfDay(ZoneId.of("UTC"))
                                                            .toInstant()))
                                    .append("adresse",
                                            new Document("numero", r.get(6))
                                                    .append("voie", r.get(7))
                                                    .append("lieuDit", r.get(5))
                                                    .append("codePostal", r.get(4))
                                                    .append("commune", r.get(2)))
                                    .append("location",
                                            new Document("type", "Point")
                                                    .append("coordinates", Arrays.asList(Double.valueOf(r.get(9)), Double.valueOf(r.get(10)) )));

                            collection.insertOne(doc);
                        });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            path = Paths.get("src/main/resources/batch/csv", "equipements.csv");
            source = Files.newBufferedReader(path, Charset.forName("UTF-8"));

            try (BufferedReader reader = new BufferedReader(source)) {
                reader.lines()
                        .skip(1)
                        .map(line -> Arrays.asList(line.split(SEPARATOR)))
                        .collect(Collectors.toList())
                        .forEach(r -> {
                            Document doc = new Document("numero", r.get(4))
                                    .append("nom", r.get(5).replace("\"", ""))
                                    .append("type", r.get(7))
                                    .append("famille", r.get(9));
                            collection.updateOne(
                                    new Document().append("_id", r.get(2)),
                                    new Document().append("$addToSet",
                                            new Document().append("equipements", doc))
                            );
                        });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            path = Paths.get("src/main/resources/batch/csv", "activites.csv");
            source = Files.newBufferedReader(path, Charset.forName("UTF-8"));

            try (BufferedReader reader = new BufferedReader(source)) {
                reader.lines()
                        .skip(1)
                        .map(line -> Arrays.asList(line.split(SEPARATOR)))
                        .map(line -> line.stream().map(i -> i.replaceAll("\"", "")).collect(Collectors.toList()))
                        .collect(Collectors.toList())
                        .forEach(r -> collection.updateOne(
                                    new Document().append("equipements", new Document("$elemMatch", new Document("numero", r.get(2).trim()))),
                                    new Document().append("$push", new Document().append("equipements.$.activites", r.get(5).trim())))
                        );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            // add index for search
            db.getCollection("installations").createIndex(new Document("location", "2dsphere"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
