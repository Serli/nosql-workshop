package nosql.workshop.batch.mongodb;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Julie on 09/02/2016.
 */
public class CsvToElasticSearch {

    public static final String COL_VILLES = "towns";

    public static void main(String[] args) {
        JestClient client = ESConnectionUtil.createClient("");

        Bulk bulk = new Bulk.Builder()
                .defaultIndex(COL_VILLES)
                .defaultType("town")
                .addAction(importCitiesFromCSVToElastic())
                .build();
        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getColumnValue(String column) {
        return column.matches("\".*\"")?column.substring(1,column.length()-1):column;
    }

    private static List<Index> importCitiesFromCSVToElastic() {
        InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        List<Index> indexes = new ArrayList<>();
        reader.lines().skip(1).filter(line -> line.length() > 0)
                .map(line -> line.split(","))
                .forEach(columns -> {
                    List<Double> coord = new ArrayList<Double>();
                    coord.add(Double.parseDouble(getColumnValue(columns[6])));
                    coord.add(Double.parseDouble(getColumnValue(columns[7])));
                    Map<String, Object> source = new HashMap<String, Object>();
                    source.put("townName", getColumnValue(columns[2]));
                    source.put("location", coord);
                    indexes.add(new Index.Builder(source).id(columns[0]).build());
                });
        return indexes;
    }
}
