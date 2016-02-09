package nosql.workshop.batch.mongodb; /*
 * ${FILE_NAME}
 * author:   Maxime Perocheau
 * created:  2016 février 09 @ 12:05
 * modified: 2016 février 09 @ 12:05
 *
 * TODO : description
 */


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.*;

public class TownsImporter {

    private static String pathTowns = "./src/main/resources/batch/csv/towns_paysdeloire.csv";

    public static void main (String[] args) {

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        List<DBObject> list = new ArrayList<>();

        for (Map<String, String> line : ReadCVS.run(pathTowns)) {

            DBObject town = new BasicDBObject()
                    .append("townName", line.get("TOWNNAME"))
                    .append(
                            "location",
                            Arrays.asList(
                                    Double.valueOf(line.get("X")),
                                    Double.valueOf(line.get("Y"))
                            )
                    );

            list.add(town);
        }

        Map<String, Object> source = new HashMap<String, Object>();
        source.put("town", list);
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
    }
}
