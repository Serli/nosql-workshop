package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;

import com.google.inject.Inject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {
	JestClient client;

	@Inject
	public SearchService() throws UnknownHostException {
		this.client = ESConnectionUtil.createClient("http://localhost:9200");
	}

	public List<TownSuggest> suggest(String text) {
		List<TownSuggest> townSuggests = new ArrayList<TownSuggest>();
		String query = "{\n" +
				"\"query\": {\n" +
				"    \"wildcard\": {\n" +
				"        \"townName\": {\n" +
				"            \"value\": \"*" + text + "*\"\n" + 
				"        }\n" +
				"    }\n" +
				"}\n" +
				"}";
		Search search = new Search.Builder(query)
		.addIndex("towns")
		.addType("town")
		.build();
		try {
			SearchResult result = client.execute(search);
			List<Hit<TownSuggest, Void>> hits = result.getHits(TownSuggest.class);
			for (Hit<TownSuggest, Void> hit : hits) {
				TownSuggest ts = hit.source;
				townSuggests.add(ts);
			}
			return townSuggests;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Double[] location(String townName) {
		String query = "{\n" +
					"\"query\": {\n" +
					"    \"match\": {\n" +
					"        \"townName\": \"*" + townName + "*\"\n" +
					"    }\n" +
					"}\n" +
				"}";
		Search search = new Search.Builder(query)
		.addIndex("towns")
		.addType("town")
		.build();
		try {
			SearchResult result = client.execute(search);
			List<Hit<TownSuggest, Void>> hits = result.getHits(TownSuggest.class);
			return hits.size() == 0 ? null : hits.get(0).source.getLocation();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public List<Installation> search(String string) {
		List<Installation> installations = new ArrayList<Installation>();
		String query = "{\n" +
				"\"query\": {\n" +
				"    \"bool\": {\n" +
				"        \"must\": [{\n" +
				"            \"query_string\": {\n" +
				"                \"query\": \"" + string + "\",\n" +
				"                \"fields\": [\"_all\"],\n" +
				"                \"analyzer\": \"french\",\n" +
				"            }\n" +
				"        }]\n" +
				"    }\n" +
				"}\n" +
			"}";
		Search search = new Search.Builder(query)
		.addIndex("installations")
		.addType("installation")
		.build();
		try {
			SearchResult result = client.execute(search);
			List<Hit<Installation, Void>> hits = result.getHits(Installation.class);
			for (Hit<Installation, Void> hit : hits) {
				Installation installation = hit.source;
				installations.add(installation);
			}
			return installations;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
