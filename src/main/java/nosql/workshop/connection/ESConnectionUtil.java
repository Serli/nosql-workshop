package nosql.workshop.connection;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * Utilitaire permettant de gérer la connexion à MongoDB
 */
public abstract class ESConnectionUtil {

    public static final JestClient client = createClient();

    private ESConnectionUtil() {
    }

    private static JestClient createClient() {
        String serverUri = "http://localhost:9200";

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUri)
                .multiThreaded(true)
                .build());
        return factory.getObject();
    }
}
