package nosql.workshop.batch.mongodb;

import io.searchbox.client.JestClient;
import nosql.workshop.connection.ESConnectionUtil;

/**
 * Created by ahuberty on 10/02/2016.
 */
public class ImportTown {
    JestClient client = ESConnectionUtil.createClient("");
}
