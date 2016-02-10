package nosql.workshop.batch.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.suggest.TownSuggest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

public class BatchJob {

    public static void main(String[] args) {
        Path path = Paths.get("src/main/resources/batch/csv", "towns_paysdeloire.csv");
        JestClient client = ESConnectionUtil.createClient("");
        try {

            Reader source = Files.newBufferedReader(path, Charset.forName("UTF-8"));
            try (BufferedReader reader = new BufferedReader(source)) {
                Bulk.Builder bulkBuilder = new Bulk.Builder();
                reader.lines()
                        .skip(1)
                        .map(line -> Arrays.asList(line.split(",")))
                        .collect(Collectors.toList())
                        .forEach(f -> {
                            TownSuggest town = new TownSuggest(f.get(1).replaceAll("\"",""), Arrays.asList(Double.valueOf(f.get(6)), Double.valueOf(f.get(7))));
                            bulkBuilder.addAction(new Index.Builder(town).index("towns").type("town").build());
                        });

                client.execute(bulkBuilder.build());
                client.shutdownClient();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            client.shutdownClient();
        }

    }

}
