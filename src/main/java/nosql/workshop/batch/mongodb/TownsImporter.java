package nosql.workshop.batch.mongodb;

/*
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
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.IOException;
import java.util.*;

public class TownsImporter {

    private static String pathTowns = "./src/main/resources/batch/csv/towns_paysdeloire.csv";

    public static List<Index> getIndex(){
        List<Index> listIndex = new ArrayList<>();

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

            listIndex.add(new Index.Builder(town).id(line.get("OBJECTID")).build());
        }
        return listIndex;
    }

    public static void main (String[] args) {

        JestClient client = ESConnectionUtil.createClient("");

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("towns")
                .defaultType("town")
                .addAction(getIndex())
                .build();

        try {
            client.execute(bulk);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
