package nosql.workshop.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import org.bson.Document;
import org.elasticsearch.action.index.IndexResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Clémentine on 09/02/2016.
 */
public class MongoDbToElastic {

    public static String getColumnValue(String column) {
        return column.matches("\".*\"")?column.substring(1,column.length()-1):column;
    }

    private static void importDataMongoToElastic(DBObject object){


    }

    private static void importCitiesFromCSVToElastic(JestClient client){
        InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            reader.lines().skip(1).filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        List<Double> coord = new ArrayList<Double>();
                        coord.add( Double.parseDouble(getColumnValue(columns[6])));
                        coord.add( Double.parseDouble(getColumnValue(columns[7])));
                        Map<String, Object> source = new HashMap<String, Object>();
                        source.put("townName",getColumnValue(columns[2]));
                        source.put("location", coord);
                        Bulk bulk = new Bulk.Builder()
                                .defaultIndex("towns")
                                .defaultType("town")
                                .addAction(new Index.Builder(source).build())
                                .build();
                        try {
                            client.execute(bulk);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    });

    }

    public static void main(String[] args) {
        JestClient client= ESConnectionUtil.createClient("");
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        DB db = mongoClient.getDB( "nosql-workshop" );

        importCitiesFromCSVToElastic(client);

        //DBCursor cursor=db.getCollection("installations").find();

        /*while(cursor.hasNext()){
            importDataMongoToElastic(cursor.next());

        }*/
    }


}
