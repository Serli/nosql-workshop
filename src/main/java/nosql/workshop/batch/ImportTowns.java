package nosql.workshop.batch;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.*;

/**
 * @author Killian
 */
public class ImportTowns {

    public static void main(String[] args) {

        // Connect ElasticSearch
        JestClient elasticClient = ESConnectionUtil.createClient();

        try (
                // Get and read CSV file
                InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        ) {

            // Creates bulk builder
            Bulk.Builder bulkBuilder = new Bulk.Builder()
                    .defaultIndex("towns")
                    .defaultType("town");

            // Iterates over the lines
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        // Add action to add current line in ElasticSearch
                        addAction(bulkBuilder, columns);
                    });

            // Insert data into ElasticSearch
            try {
                elasticClient.execute(bulkBuilder.build());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        elasticClient.shutdownClient();

    }

    /**
     * Adds an action to the given bulk builder.
     * @param bulkBuilder bulk builder
     * @param values values to add
     */
    private static void addAction(Bulk.Builder bulkBuilder, String[] values) {
        try {
            String jsonValues = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("townname", values[1])
                    .field("townname_suggest", values[2])
                    .field("postcode", values[3])
                    .field("pays", values[4])
                    .field("region", values[5])
                    .field("x", values[6])
                    .field("y", values[7])
                    .endObject()
                    .string();
            bulkBuilder.addAction(new Index.Builder(jsonValues).id(values[0]).build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
