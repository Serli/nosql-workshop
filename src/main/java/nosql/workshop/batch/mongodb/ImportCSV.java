package nosql.workshop.batch.mongodb;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import nosql.workshop.model.Installation;
import nosql.workshop.services.MongoDB;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

public class ImportCSV {

    String activitesCSV = "./src/main/resources/batch/csv/activites.csv";
    String equipementsCSV = "./src/main/resources/batch/csv/equipements.csv";
    String installationsCSV = "./src/main/resources/batch/csv/installations.csv";

    MongoCollection<Document> coll;

    public static void main(String[] args) {

        ImportCSV obj = new ImportCSV();
        obj.run();

    }

    public void run(){

        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("nosql-workshop");
        this.coll = db.getCollection("installations");

        this.coll.drop();
        this.readInstallations();
        this.readEquipements();
        this.readActivites();
    }

    public void readInstallations() {

        BufferedReader br = null;
        String cvsSplitBy = "\",\"";

        try {
            
            br = new BufferedReader(new FileReader(installationsCSV));

            br.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(cvsSplitBy))
                    .forEach(column -> {

                        Document doc = new Document("_id", column[1])
                                .append("nom", column[0])
                                .append("adresse",
                                        new Document("numero", column[6])
                                        .append("voie", column[7])
                                        .append("lieuDit", column[5])
                                        .append("codePostal", column[4])
                                        .append("commune", column[2]))
                                .append("location",
                                        new Document("type", "Point")
                                        .append("coordinates", Arrays.asList(Float.valueOf(column[9]), Float.valueOf(column[10]))))
                                .append("multiCommune", "Oui".equals(column[16]))
                                .append("nbPlacesParking",  column[17].isEmpty() ? null : Integer.valueOf(column[17]))
                                .append("nbPlacesParkingHandicapes", column[18].isEmpty() ? null : Integer.valueOf(column[18]))
                                .append("dateMiseAJourFiche", column[28] );

                        coll.insertOne(doc);
                    });

            System.out.println("Insertion des installations terminee");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("Done");
    }

    public void readEquipements() {

        BufferedReader br = null;
        //String line = "";
        String cvsSplitBy = ",";

        try {

            br = new BufferedReader(new FileReader(equipementsCSV));

            br.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(column -> {

                        coll.updateOne(new Document("_id", column[2].trim()),
                                new Document("$push", new Document("equipements", new Document()
                                .append("numero", column[4])
                                .append("nom", column[5])
                                .append("type", column[7])
                                .append("famille", column[9]))));


                    });

            System.out.println("Insertion des equipements terminee");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("Done");
    }

    public void readActivites() {

        BufferedReader br = null;
        //String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new FileReader(activitesCSV));

            br.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split("\",\""))
                    .forEach(column -> {

                        coll.updateOne(
                                new Document("equipements",
                                        new Document("$elemMatch",
                                                new Document("numero", column[2].trim()))),

                                new Document("$push",
                                        new Document("equipements.$.activites", column[5])));



                    });

            System.out.println("Insertion des activites terminee");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("Done");
    }
}
