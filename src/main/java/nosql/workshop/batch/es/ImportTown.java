package nosql.workshop.batch.es;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.IndicesExists;
import nosql.workshop.connection.ESConnectionUtil;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author Marion Bechennec
 */
public class ImportTown {

    public static final String ES_INDEX = "nosql-workshop";
    private final Logger LOGGER = Logger.getLogger(getClass().getName());

    public static void main(String[] args) {
        ImportTown importer = new ImportTown();
        importer.importTowns();
    }

    private void importTowns() {

        JestClient jestClient = ESConnectionUtil.createClient("");
        try {
            boolean exists = jestClient.execute(new IndicesExists.Builder(ES_INDEX).build()).isSucceeded();
            if (exists) {
                // do smt
                InputStream is = getClass().getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                Bulk.Builder bulkIndexBuilder = new Bulk.Builder();

                br.lines()
                        .skip(1)
                        .filter(line -> line.length() > 0)
                        .map(line -> line.split(","))
                        .forEach(column -> {

                            String id = cleanString(column[0]);
                            String name = cleanString(column[1]);
                            String nameSuggest = cleanString(column[2]);
                            List<Double> location = Arrays.asList(Double.valueOf(cleanString(column[6])), Double.valueOf(cleanString(column[7])));

                            Town town = new Town(name, nameSuggest, location);

                            bulkIndexBuilder.addAction(new Index.Builder(town).index(ES_INDEX).type("towns").build());
                        });

                try {
                    BulkResult bulkResult = jestClient.execute(bulkIndexBuilder.build());
                    for (BulkResult.BulkResultItem bulkResultItem : bulkResult.getFailedItems()) {
                        LOGGER.severe("Error when processing the bulk: "+ bulkResultItem.error);

                    }


                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String cleanString(String s) {
        String value = s.matches("\".*\"") ? s.substring(1, s.length() - 1) : s;
        return value.trim();
    }

    private class Town {
        private final String name;
        private final String nameSuggest;
        private final List<Double> location;

        public Town(String name, String nameSuggest, List<Double> location) {

            this.name = name;
            this.nameSuggest = nameSuggest;
            this.location = location;
        }
    }
}
