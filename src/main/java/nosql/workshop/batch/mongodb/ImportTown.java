package nosql.workshop.batch.mongodb;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ahuberty on 10/02/2016.
 */
public class ImportTown {

    private static final String pathTowns = "/batch/csv/towns_paysdeloire.csv";

    public static void main(String[] args) {
        List<Document> towns = new ArrayList<>();
        try (InputStream inputStream = ImportTown.class.getResourceAsStream(pathTowns);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        towns.add(new Document()
                                        .append("_id", getColumnValue(columns[0]))
                                        .append("townName", getColumnValue(columns[1]))
                                        .append("location", Arrays.asList(
                                                Double.valueOf(getColumnValue(columns[6])),
                                                Double.valueOf(getColumnValue(columns[7]))))
                        );
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }


        JestClient jestClient = ESConnectionUtil.createClient("");
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("towns")
                .defaultType("town")
                .addAction(getTowns(towns))
                .build();

        try {
            jestClient.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Index> getTowns(List<Document> towns) {
        List<Index> res = new ArrayList<>();
        for (Document town : towns) {
            res.add(createIndex(town));
        }
        return res;
    }

    private static Index createIndex(Document town) {
        String id = town.get("_id").toString();
        town.remove("_id");
        return new Index.Builder(town).id(id).build();
    }

    private static String getColumnValue(String columnElt) {
        return columnElt.matches("\".*") ? columnElt.substring(1, columnElt.length() - 1) : columnElt;
    }
}
