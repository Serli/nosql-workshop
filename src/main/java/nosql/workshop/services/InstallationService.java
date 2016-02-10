package nosql.workshop.services;

import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.util.JSON;

import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import com.mongodb.BasicDBObject;

import net.codestory.http.Context;
import nosql.workshop.model.stats.InstallationsStats;

import org.jongo.Find;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.omg.CORBA.SystemException;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.function.Consumer;

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
    	int nb = (int) installations.count();
    	Installation installation = installations.find().skip((int) (Math.random()*nb + 1)).as(Installation.class).next();
    	return installation;
    }
    
    public List<Installation> getAll(){
    	List<Installation> installationsArray = new ArrayList<Installation>();
    	Consumer<Installation> action = (Installation s) -> installationsArray.add(s);
    	
    	installations.find().as(Installation.class).forEach(action);
    	
    	return installationsArray;
    }
    
    public Installation get(String number){
    	Installation installation = installations.findOne(String.format("{_id: '%s'}", number)).as(Installation.class);
    	return installation;
    }
    
    public List<Installation> search(String search){
    	JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.query(QueryBuilders.queryString(search));
    	 
    	Search searching = (Search) new Search.Builder(searchSourceBuilder.toString())
    	                                // multiple index or types can be added.
    	                                .addIndex("installations")
    	                                .addType("installation")
    	                                .build();
    	 
    	try {
			JestResult result = client.execute(searching);
			System.out.println(result.getSourceAsObjectList(Installation.class));
			return result.getSourceAsObjectList(Installation.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }

    public List<Installation> geosearch(Context context) {
    	double lat = Double.parseDouble(context.get("lat"));
    	double lng = Double.parseDouble(context.get("lng"));
    	int distance = Integer.parseInt(context.get("distance"));
    	
    	List<Installation> installationsArray = new ArrayList<Installation>();
    	Consumer<Installation> action = (Installation s) -> installationsArray.add(s);
    	
    	installations.find(String.format("{\"location\": {$near : {$geometry : { type : \"Point\", coordinates: [%s, %s]}, $maxDistance : %s}}}",lat, lng, distance)).as(Installation.class).forEach(action);
    	return installationsArray;
    }
    
    public InstallationsStats stats(){
    	int totalCount = (int) installations.count();
    	installations.aggregate("[{$group : {_id : \"$nbequ\", total : {$sum : 1}}}]");
    	return null;
    }
}
