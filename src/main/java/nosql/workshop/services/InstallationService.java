package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.BasicDBObject;

import net.codestory.http.Context;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.FindOne;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static nosql.workshop.model.Installation.*;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "collectionInstallationsSportives";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
    	return installations.findOne().as(Installation.class);
    }
    
    public Installation byId(String numero) {
    	return installations.findOne("{\"_id\": \""+numero+"\"}").as(Installation.class);
    }
    public InstallationsStats stats() {
        return null;
    }
    
    public List<Installation> search(Context context) {
        return null;

    }
}
