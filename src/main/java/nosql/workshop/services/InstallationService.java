package nosql.workshop.services;


import com.google.common.collect.Lists;
import com.google.gson.JsonObject;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mongodb.util.JSON;

import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;

import org.apache.lucene.util.fst.Builder;
import nosql.workshop.model.stats.CountByActivity;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;

import com.mongodb.BasicDBObject;

import net.codestory.http.Context;

import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.model.suggest.TownSuggest;

import org.jongo.MongoCollection;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

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
    
    public List<TownSuggest> searchTown(String search){
    	JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.query(QueryBuilders.queryString(search));
    	 
    	Search searching = (Search) new Search.Builder(searchSourceBuilder.toString())
    	                                // multiple index or types can be added.
    	                                .addIndex("towns")
    	                                .addType("town")
    	                                .build();
    	 
    	try {
			JestResult result = client.execute(searching);
			System.out.println(result.getSourceAsObjectList(TownSuggest.class));
			return result.getSourceAsObjectList(TownSuggest.class);
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
    	// installations.aggregate("[{$project: { equipements: 1}} $group: { _id: \"$department\",average: { $avg: \"$sum\" } }]").as(Installation.class);
    	
    	double avg_equ = 0;
    	int max_equ = 0;
    	
    	ArrayList<Installation> list = new ArrayList<Installation>();
    	
    	Iterator<CountByActivity> countbyActivitiesIt = installations.aggregate(
				"{$match: {\"equipements.activites\":{$exists : true}}}")
				.and("{$unwind: \"$equipements\"}")
				.and("{$unwind: \"$equipements.activites\"}")
				.and("{$group: {_id:\"$equipements.activites\", total: {$sum:1}}}")
				.and("{$project: {_id:0, activite:\"$_id\", total:1}}")
				.and("{$sort: {total: -1}}")
				.as(CountByActivity.class);
    	
    	ArrayList<CountByActivity> activities = Lists.newArrayList(countbyActivitiesIt);

    	Consumer<Installation> action = (Installation s) -> list.add(s);
    	installations.find().as(Installation.class).forEach(action);
    	
    	for (int i = 0 ; i < list.size() ; i++){
    		avg_equ += list.get(i).getEquipements().size();
    		if (list.get(i).getEquipements().size() > max_equ){
    			max_equ = list.get(i).getEquipements().size();
    		}
    	}
    	avg_equ = avg_equ/totalCount;
    	
    	Installation max_install = installations.findOne(String.format("{_nbequ: '%s'}", max_equ)).as(Installation.class);
    	
    	InstallationsStats stats = new InstallationsStats();
    	stats.setAverageEquipmentsPerInstallation(avg_equ);
    	stats.setTotalCount(totalCount);
    	stats.setInstallationWithMaxEquipments(max_install);
    	stats.setCountByActivity(activities);
    	return stats;
    }
    
    public List<TownSuggest> suggest(String search){
    	JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.suggest().addSuggestion(SuggestBuilders.completionSuggestion(search));
    	String query =  String.format("{\"size\": 0, \"suggest\" : {\"my-suggestions\" : {\"text\" : \"%s\",\"term\" : {\"size\" : 3,\"field\" : \"townName\"}}}}", search) ;
    	Search searching = (Search) new Search.Builder(query)
    	                                // multiple index or types can be added.
    	                                .addIndex("towns")
    	                                .addType("town")
    	                                .build();
    	
    	JestResult result;
    	String ville="";
		try {
			result = client.execute(searching);
			System.out.println(result.getJsonString());
			// Not the good way to retrieve an object with a JSON, with more time, change it
			// DO it better to retrived everything
			try{
				ville =result.getJsonString().substring(result.getJsonString().indexOf("\"options\":[{\"text\"")+20, result.getJsonString().indexOf("\"score\"")-2);
			}
			catch(Exception e){}
			return this.searchTown(ville);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }
    
    public Double[] getLocation(String search){
    	JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.query(QueryBuilders.queryString(search));
    	 
    	Search searching = (Search) new Search.Builder(searchSourceBuilder.toString())
    	                                // multiple index or types can be added.
    	                                .addIndex("towns")
    	                                .addType("town")
    	                                .build();
    	 
    	try {
			JestResult result = client.execute(searching);
			System.out.println(result.getSourceAsObjectList(TownSuggest.class));
			return (result.getSourceAsObjectList(TownSuggest.class)).get(0).getLocation();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }
    
}
