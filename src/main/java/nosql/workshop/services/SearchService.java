package nosql.workshop.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import nosql.workshop.model.Town;
import nosql.workshop.model.suggest.TownSuggest;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

	public Double[] getLocation(String townName) throws IOException {

		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.build());
		JestClient client = factory.getObject();

		String q = "{\n" +
				"        \"query\": {\n"+
				"           \"match\": {\n"+
				"               \"townName\": \"" + townName + "\" \n" +
				"           }\n" +
				"       }\n" +
				"}";

		Search search = (Search) new Search.Builder(q)
				.addIndex("towns")
				.addType("town")
				.build();

		JestResult result = client.execute(search);

		JsonObject object = result.getJsonObject();
		JsonArray finds = object.get("hits").getAsJsonObject().get("hits").getAsJsonArray();
		if(finds.size() > 0){
			JsonObject firstFind = finds.get(0).getAsJsonObject();
			JsonObject town = firstFind.get("_source").getAsJsonObject();
			JsonArray location = town.get("location").getAsJsonArray();
			Double[] res = {location.get(0).getAsDouble(), location.get(1).getAsDouble()};
			return res;
		}
		else
			return null;		
	}

	public List<TownSuggest> suggest(String text) throws IOException {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.build());
		JestClient client = factory.getObject();
		String q = "{\n" +
                "        \"query\": {\n"+
                "           \"wildcard\": {\n"+
                "               \"townName\": {\n"+
                "                   \"value\": \"*" + text.toLowerCase() + "*\" \n" +
                "               }\n"+
                "           }\n" +
                "       }\n" +
                "}";

        Search search = (Search) new Search.Builder(q)
                .addIndex("towns")
                .addType("town")
                .build();

        JestResult res = client.execute(search);

        return res.getSourceAsObjectList(TownSuggest.class);
	}

}
