package nosql.workshop.utils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * Utilitaire permettant de gérer la connexion à ElasticSearch
 */
public abstract class JestConnection {

    private JestConnection() {
    }

    public static JestClient createClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(180000)
                .build());
        return factory.getObject();
    }

}
