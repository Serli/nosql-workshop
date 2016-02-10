package nosql.workshop.services;


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Installation.Adresse;
import nosql.workshop.model.Installation.Location;
import nosql.workshop.model.Town;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;
import org.jongo.MongoCollection;

import net.codestory.http.Context;
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
		return installations.aggregate("{ $sample: { size: 1 } }").as(Installation.class).next();
	}

	public Installation getInstallationByNumero(String numero) {       	
		Installation inst = installations.findOne(String.format("{'_id':'%s'}", numero)).as(Installation.class);		
		return inst;    	
	}

	public List<Installation> getAllInstallations() {
		MongoCursor<Installation> installs = installations.find().as(Installation.class);
		ArrayList<Installation> installationsList = new ArrayList<Installation>();
		for(Installation inst : installs) {
			installationsList.add(inst);
		}
		return installationsList;
	}

	public List<Installation> search(String query) throws IOException {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.build());
		JestClient client = factory.getObject();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.queryString(query));

		String q = "{\"query\": { "
				+ " \"match\": { "
				+ " \"nom\":\"" + query + "\""
				+ "}"
				+ "}}";

		Search search = (Search) new Search.Builder(q)
				.addIndex("installations")
				.addType("installation")              
				.build();      


		JestResult result = client.execute(search);

		List<Installation> res = result.getSourceAsObjectList(Installation.class);
		return res ;
	}

	public List<Installation> geosearch(String lat, String lng, String distance) throws IOException{
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.build());
		JestClient client = factory.getObject();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.queryString(lat))
		.query(QueryBuilders.queryString(lng))
		.query(QueryBuilders.queryString(distance));
		Search search = (Search) new Search.Builder(searchSourceBuilder.toString())
				.addIndex("installations")
				.addType("installation")
				.build();        
		JestResult result = client.execute(search);

		List<Installation> res = result.getSourceAsObjectList(Installation.class);
		return res ;

	}


	public List<Installation> geoSearch(String lat, String lng, String distance) {
		installations.ensureIndex("{ location : '2dsphere' } ");
		return Lists.newArrayList(installations.find("{'location' : { $near : { $geometry : { type : 'Point', coordinates: ["+lng+", "+lat+"]}, $maxDistance : "+distance+"}}}")
				.as(Installation.class).iterator());
	}


	public List<Installation> getList() {
		return Lists.newArrayList(installations.find().as(Installation.class).iterator());
	}	

	public InstallationsStats getInstallationsStat() {
		InstallationsStats stats = new InstallationsStats();

		long total = installations.count();

		stats.setTotalCount(total);

		Installation installation = installations.aggregate("{"
				+ "$project: {equip: {$size: '$equipements'}}}")
				.and("{$sort: {equip: -1}"
						+ "}")
				.and("{$limit: 1}")
				.as(Installation.class).next();
		Installation maxInstall = installations.findOne("{_id: #}",
				installation.get_id())
				.as(Installation.class);

		stats.setInstallationWithMaxEquipments(maxInstall);

		double moy = installations.aggregate("{"
				+ "$unwind: '$equipements'"
				+ "}")
				.and("{"
						+ "$group: {_id : '$_id', total: {$sum : 1}}"
						+ "}")
				.and("{"
						+ "$group: {_id : 0, average: {$avg : '$total'}}"
						+ "}")
				.and("{"
						+ "$project: {_id : 0, average: 1}"
						+ "}")
				.as(Average.class).next().getAverage();

		stats.setAverageEquipmentsPerInstallation(moy);

		ArrayList<CountByActivity> listCount = Lists.newArrayList(installations.aggregate("{"
				+ "$unwind:'$equipements'"
				+ "}")
				.and("{"
						+ "$unwind: '$equipements.activites'"
						+ "}")
				.and("{"
						+ "$group: {_id: '$equipements.activites', total:{$sum : 1}}"
						+ "}")
				.and("{"
						+ "$project: {activite: '$_id', total : 1}"
						+ "}")
				.and("{"
						+ "$sort: {total: -1}"
						+ "}")
				.as(CountByActivity.class).iterator());

		stats.setCountByActivity(listCount);

		return stats;
	}

}