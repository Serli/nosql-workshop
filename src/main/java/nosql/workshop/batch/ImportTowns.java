package nosql.workshop.batch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.*;

/**
 * @author Killian
 */
public class ImportTowns {

    public static void main(String[] args) {

        // Connect ElasticSearch
        JestClientFactory jestFactory = new JestClientFactory();
        jestFactory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient elasticClient = jestFactory.getObject();

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
                        try {
                            // Add action to add current line in ElasticSearch
                            bulkBuilder.addAction(new Index.Builder(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field("townname", columns[1])
                                    .field("townname_suggest", columns[2])
                                    .field("postcode", columns[3])
                                    .field("pays", columns[4])
                                    .field("region", columns[5])
                                    .field("x", columns[6])
                                    .field("y", columns[7])
                                    .endObject()
                                    .string()
                            ).id(columns[0]).build());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
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

}
