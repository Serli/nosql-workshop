package nosql.workshop.batch.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by chriswoodrow on 09/02/2016.
 * Modified by Théophile Morin & Remy Ferre
 */
public class CsvToMongoDb {
    public static void main(String[] args) {
        MongoCollection installations = getCollection();
        long finish, end, beg = System.currentTimeMillis(), start = System.currentTimeMillis();

        System.out.println("Computing installations...");
        computeInstallations(installations);

        end = System.currentTimeMillis();
        System.out.println("Done in " + (end - beg) + "ms.");

        System.out.println("Computing equipments...");
        beg = System.currentTimeMillis();
        computeEquipements(installations);
        end = System.currentTimeMillis();
        System.out.println("Done in " + (end - beg) + "ms.");

        System.out.println("Computing activities...");
        beg = System.currentTimeMillis();
        computeActivites(installations);
        end = System.currentTimeMillis();
        System.out.println("Done in " + (end - beg) + "ms.");

        finish = System.currentTimeMillis();
        System.out.println("Batch executed in " + (finish - start) + "ms.");

        ensureIndexes();
    }

    private static MongoCollection getCollection() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        MongoCollection installations = db.getCollection("installations");
        installations.drop();
        return installations;
    }

    private static void ensureIndexes() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        //db.runCommand("");

    }

    private static void computeInstallations(MongoCollection installations) {
        List<Document> instList;

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            instList = reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .map(columns -> mapInstallationCSVtoDBObject(columns))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        installations.insertMany(instList);
    }

    private static void computeEquipements(MongoCollection installations) {
        Map<Document, String> equipMap = new HashMap<>();

        try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/equipements.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> computeEquipmentLine(equipMap, columns));

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        for(Map.Entry<Document, String> entry : equipMap.entrySet()){
            installations.findOneAndUpdate(new Document("_id", entry.getValue()),
                    new Document("$addToSet", new Document("equipements", entry.getKey())));
        }
    }

    private static void computeEquipmentLine(Map<Document, String> map, String[] line) {
        Document d = mapEquipementCSVtoDBObject(line);
        String ref = line[2];
        map.put(d, ref);
    }

    private static void computeActivites(MongoCollection installations) {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/batch/csv/activites.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        br.lines()
                .skip(1)
                .filter(line -> line.length() > 0)
                .map(line -> line.split(","))
                .forEach(column -> {
                    String numEquip = cleanString(column[2]);
                    String nomActivite = cleanString(column[5]);

                    UpdateResult updateResult = installations.updateOne(
                            new Document().append("equipements",
                                    new Document("$elemMatch",
                                            new Document("numero", numEquip)
                                    )
                            ),
                            new Document().append("$addToSet",
                                    new Document().append("equipements.$.activites", nomActivite)
                            )
                    );
                });
    }


    private static Document mapInstallationCSVtoDBObject(String[] csvData) {
        try {
          /*  List<String> elemsList = new LinkedList<>();
            Stream<String> a = Arrays.stream(line).filter(e -> e.length()>=2).map(e -> e.substring(1, e.length()-1));
            String[] csvData = a.toArray(String[]::new);*/

            Document addr = new Document("commune", csvData[2])
                    .append("codePostal", "".equals(csvData[4])? 0 : Integer.parseInt(csvData[4]))
                    .append("numero", csvData[6])
                    .append("voie", csvData[7])
                    .append("lieuDit",  csvData[5]);

            Document loc = new Document("type", "Point")
                    .append("coordinates", Arrays.asList(Float.parseFloat(csvData[9]),Float.parseFloat(csvData[10])));

            //Date date; date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")).parse(csvData[28].substring(0, csvData[28].length() - 1));
            Document inst = new Document("_id", csvData[1])
                    .append("nom", csvData[0].substring(1))
                    .append("adresse", addr)
                    .append("multiCommune", "oui".equalsIgnoreCase(csvData[16]))
                    .append("nbPlacesParking", "".equals(csvData[17]) ? 0 : Integer.parseInt(csvData[17]))
                    .append("nbPlacesParkingHandicapes", "".equals(csvData[18]) ? 0 : Integer.parseInt(csvData[18]))
                    .append("dateMiseAJourFiche", csvData[28].substring(0, csvData[28].length() - 1))
                    .append("location", loc)
                    .append("equipements", Arrays.asList());

            return inst;
        }catch(Exception e) {
            System.err.println(e.toString());
            return null;
        }
    }


    private static Document mapEquipementCSVtoDBObject(String[] csvData) {
        try {
            int offset = 0;
            String numero;
            try {
                numero = csvData[4];
            }catch(Exception e) {
                System.err.println(e.toString());
                while(!csvData[4+offset].substring(csvData[4+offset].length()-1, csvData[4+offset].length()).equals("\"")) {
                    ++offset;
                    System.err.println("Retrying for: " + csvData[4+offset]);
                }
                ++offset;
                System.err.println("Retrying for: " + csvData[4+offset]);
                numero = csvData[4+offset];
            }
            Document equip = new Document("numero", numero)
                    .append("nom", csvData[5+offset])
                    .append("type", csvData[7+offset])
                    .append("famille", csvData[9+offset]);
            // .append("activites", Arrays.asList());

            return equip;
        }catch(Exception e) {
            System.err.println(e.toString());
            return null;
        }
    }

    private static String cleanString(String s) {
        String value = s.matches("\".*\"") ? s.substring(1, s.length() - 1) : s;
        return value.trim();
    }
}
