package nosql.workshop.batch.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import nosql.workshop.model.Town;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Théophile Morin & Remy Ferre
 */
public class CsvToElasticSearch {


    public static void main(String[] args) throws IOException {

        final JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(400000)
                .build());
        JestClient client = factory.getObject();

        //Create index
        client.execute(new CreateIndex.Builder("towns").build());
        List<Town> townList;

        //Parse CSV
        try (InputStream inputStream = CsvToElasticSearch.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            townList = reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .map(columns -> mapTownCSVtoPojo(columns))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        System.out.println("Town list contains " + townList.size() + " entries.");
        System.out.println("Bulk insert...");

        Bulk.Builder bulk = new Bulk.Builder()
                .defaultIndex("towns")
                .defaultType("town");
        for (Town t : townList) {
            bulk.addAction(createIndex(t));
        }
        Bulk BulkExec = bulk.build();

        try {
            client.execute(BulkExec);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Batch successfully executed.");
    }

    //Not used
    private static List<Index> getIndexes(List<Town> townList) {
        List<Index> indexes = new ArrayList<>();
        for (Town t : townList) {
            indexes.add(createIndex(t));
        }
        return indexes;
    }

    /**
     * Create elastic Index from Town
     * @param t - The town
     * @return an ElasticSearch index
     */
    private static Index createIndex(Town t) {
        String id = t.getId();
        return new Index.Builder(t).id(id).build();
    }

    /**
     * Build a Town object from a String[].
     * @param line A line from the CSV
     * @return The corresponding Town
     */
    public static Town mapTownCSVtoPojo(String[] line) {

        Town t = new Town();
        t.setId(line[0]);
        t.setName(cleanString(line[1]));
        t.setSugggest(cleanString(line[2]));
        t.setPostCode(cleanString(line[3]));
        t.setPays(cleanString(line[4]));
        t.setRegion(cleanString(line[5]));
        List<Float> loc = new ArrayList<>(2);
        loc.add(Float.parseFloat(line[6]));
        loc.add(Float.parseFloat(line[7]));
        t.setLocation(loc);

        return t;
    }

    /**
     * Remove '"' at the prefix and suffix of a String.
     * @param s - The String to clean
     * @return The cleaned String
     */
    private static String cleanString(String s) {
        String value = s.matches("\".*\"") ? s.substring(1, s.length() - 1) : s;
        return value.trim();
    }

}
