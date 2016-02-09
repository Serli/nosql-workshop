package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.searchbox.client.JestClient;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import org.bson.types.ObjectId;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static nosql.workshop.model.Installation.*;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
@SuppressWarnings("unchecked")
public class InstallationService {

    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;
    private final JestClient elasticClient;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
        this.elasticClient = ESConnectionUtil.createClient();
    }

    public Installation random() {
        return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
    }

    public Installation get(String numero) {
        return installations.findOne(new ObjectId(numero)).as(Installation.class);
    }

    public List<Installation> list() {
        MongoCursor<Installation> cursor = installations.find().as(Installation.class);
        List<Installation> list = new ArrayList<>();
        cursor.forEach(installation -> list.add(installation));
        return list;
    }

    public List<Installation> list(String search) {

        return null;
    }

}
