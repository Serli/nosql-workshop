package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.google.inject.Inject;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

	private JestClient client = ESConnectionUtil.createClient("http://localhost:9200");


	public List<TownSuggest> suggest(String text) {
		List<TownSuggest> aRetourner = new ArrayList<TownSuggest>();
		//Searching
		String query = "{\n" +
				"    \"query\": {\n" +
				"        \"filtered\" : {\n" +
				"            \"query\" : {\n" +
				"                \"query_string\" : {\n" +
				"                    \"query\" : \""+text+"\"\n" +
				"                }\n" +
				"            }\n"+
				"        }\n" +
				"    }\n" +
				"}";
		Search search = (Search) new Search.Builder(query)
				.addIndex("towns")
				.addType("town")
				.build();
		JestResult result = null;
		try {
			result = client.execute(search);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<Installation> installationsAfterResult = result.getSourceAsObjectList(Installation.class);
		//for(int i = 0;installationsAfterResult.leng)

		return aRetourner;
}

public Double[] getLocation(String townName) {
	// TODO Auto-generated method stub
	return null;
}

}
