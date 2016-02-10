package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * Created by romanlp on 09/02/16.
 */
public class MongoToElastic {

    String townCsv = "./src/main/resources/batch/csv/towns_paysdeloire.csv";

    private String connectionUrl = "http://localhost:9200";


    MongoCollection<Document> coll;

    public void run(){

        this.importMongoToElastic();
        this.importCity();

    }

    private void importCity() {

        BufferedReader br = null;
        String cvsSplitBy = ",";

        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection collection = db.getCollection("installations");
        DBCursor cursor = collection.find();

        try {

            br = new BufferedReader(new FileReader(townCsv));

            List<Document> documents = new ArrayList<>();

            br.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(cvsSplitBy))
                    .forEach(column -> {

                        Document obj = new Document("townName", column[1].trim().replace("\"", ""))
                                .append("id", column[0].trim())
                                .append(
                                        "location",
                                        Arrays.asList(
                                                Double.valueOf(column[6]),
                                                Double.valueOf(column[7])
                                        )
                                );
                        documents.add(obj);

                    });

            JestClient client = ESConnectionUtil.createClient("");

            Bulk bulk = new Bulk.Builder()
                    .defaultIndex("towns")
                    .defaultType("town")
                    .addAction(getTowns(documents))
                    .build();

            try {
                client.execute(bulk);
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Importation des villes terminée");

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

    private static List<Index> getTowns(List<Document> list){
        List<Index> indexes = new ArrayList<>();
        for (Document object: list) {

            String id = object.get("id").toString();

            indexes.add(new Index.Builder(object).id(id).build());
        }
        return indexes;
    }



    //IMPORT DE MONGO VERS ELASTIC SEARCH


    public void importMongoToElastic(){

        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection collection = db.getCollection("installations");
        DBCursor cursor = collection.find();

        JestClient client = ESConnectionUtil.createClient("");

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("installations")
                .defaultType("installation")
                .addAction(getListBulkable(cursor))
                .build();

        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Collection<Index> getListBulkable(DBCursor cursor){
        ArrayList<Index> list = new ArrayList<>();
        while(cursor.hasNext()){
            list.add(createIndex(cursor.next()));
        }
        return list;
    }

    private static Index createIndex(DBObject object){
        String id = object.get("_id").toString();
        object.removeField("_id");
        object.put("id", id);
        object.removeField("dateMiseAJourFiche");
        return new Index.Builder(object).id(id).build();
    }





}
