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
}
