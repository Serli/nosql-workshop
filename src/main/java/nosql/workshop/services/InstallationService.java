package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.DB;
import com.mongodb.MongoClient;

import com.mongodb.client.MongoDatabase;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import org.bson.Document;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        Jongo jongo = new Jongo(db);
        MongoCollection installations = jongo.getCollection("installations");
        long size = installations.count();
        Installation installation = installations.find().limit(1).skip((int) Math.floor(Math.random() * size)).as(Installation.class).next();
        return installation;
    }

    public List<Installation> getInstallations(){
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("nosql-workshop");
        Jongo jongo = new Jongo(db);
        MongoCollection installations = jongo.getCollection("installations");
        MongoCursor<Installation> all = installations.find().as(Installation.class);
        List<Installation> result = new ArrayList<>();
        for(Installation installation : all){
            result.add(installation);
        }
        return result;
    }
    public Installation getId(String id){
        DB db = new MongoClient().getDB("nosql-workshop");
        Jongo jongo = new Jongo(db);
        String query = "{_id : '"+id+"'}";
        return jongo.getCollection("installations").findOne(query).as(Installation.class);
    }

}
