package nosql.workshop.connection;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * Utilitaire permettant de gérer la connexion à ElasticSearch
 */
public abstract class ESConnectionUtil {

    private ESConnectionUtil() {
    }

    public static JestClient createClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        return factory.getObject();
    }

}
