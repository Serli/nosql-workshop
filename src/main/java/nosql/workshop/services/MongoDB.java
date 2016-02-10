package nosql.workshop.services;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.jongo.Jongo;

import java.net.UnknownHostException;

/**
 * Classe utilitaire permettant de fournir une connexion à la base MongoDB.
 */
public class MongoDB {

    /**
     * Retourne une instance Jongo permettant de se connecter à la base MongoDB.
     *
     * @return l'instance Jongo.
     * @throws UnknownHostException si la base n'est pas disponible.
     */
    public Jongo getJongo() throws UnknownHostException {
        String givenUri = System.getenv("MONGOLAB_URI");
        String uri = givenUri == null ? "mongodb://localhost:27017/sportDB" : givenUri;
        MongoClientURI mongoClientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        DB db = mongoClient.getDB(mongoClientURI.getDatabase());
        return new Jongo(db);
    }
}
