package nosql.workshop.batch.elasticsearch;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.opencsv.CSVReader;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by Joris on 09/02/2016.
 */
public class TownsImporter {

    public TownsImporter(){}

    public static void main(String[] args) {
        TownsImporter townsImporter = new TownsImporter();
        townsImporter.writeToElastic();
    }

    public static List<String[]> readCSV(String path) {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(path));
            return reader.readAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public void writeToElastic(){
        List<DBObject> dbObjectList = new ArrayList<>();
        List<String[]> myEntries = readCSV("src\\main\\resources\\batch\\csv\\towns_paysdeloire.csv");
        myEntries.stream()
                .skip(1)
                .forEach(columns -> {
                    DBObject obj = new BasicDBObject()
                            .append("townName", columns[1].trim())
                            .append("id", columns[0].trim())
                            .append(
                                    "location",
                                    Arrays.asList(
                                            Double.valueOf(columns[6]),
                                            Double.valueOf(columns[7])
                                    )
                            );
                    dbObjectList.add(obj);
                });

        String connectionUrl = "http://localhost:9200";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("towns")
                .defaultType("town")
                .addAction(getTowns(dbObjectList))
                .build();

        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Index> getTowns(List<DBObject> list){
        List<Index> indexes = new ArrayList<>();
        for (DBObject object: list) {
            indexes.add(createIndex(object));
        }
        return indexes;
    }

    private static Index createIndex(DBObject object){
        String id = object.get("id").toString();
        object.removeField("id");
        return new Index.Builder(object).id(id).build();
    }
}
