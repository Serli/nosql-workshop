package nosql.workshop.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.DBObject;

import net.codestory.http.Query;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;

import org.elasticsearch.common.collect.Lists;
import org.jongo.FindOne;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
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
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
        return installations.findOne().as(Installation.class);
    }
    
    public List<Installation> list(){
    	 Iterable<Installation> list = installations.find().as(Installation.class);
    	 return Lists.newArrayList(list);
    }
    
    public Installation get(String numero){
    	return installations.findOne("{_id:'"+numero+"'}").as(Installation.class);
    }
    
    public List<Installation> search(String query){
    	Iterable<Installation> list 
    			= installations.find("{$text:{$search:'"+query+"'}}")
    					.as(Installation.class);
    	return Lists.newArrayList(list);
    }
    public List<Installation> geosearch(Query query){
    	String lat = query.get("lat");
    	String lng = query.get("lng");
    	String distance = query.get("distance");
    	String geoQuery = "{location: "
    						+ "{$near: "
    							+ "{$geometry :"
    								+ "{type: \"Point\","
    									+ "coordinates :["+lat+","+lng+"]"
    								+ "},"
    								+ "$maxDistance: "+distance+""
    							+ "}"
    						+ "}"
    					+ "}";
    	Iterator<Installation> inst 
    		= installations.find(geoQuery)
    		.as(Installation.class);
    	return Lists.newArrayList(inst);
    }
}
