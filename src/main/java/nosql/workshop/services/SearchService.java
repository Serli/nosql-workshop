package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.suggest.TownSuggest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
				"        \"wildcard\" : {\n" +
				"            \"townName\" : {\n" +               
				"                    \"value\" : \""+text+"*\"\n" +             
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
			e.printStackTrace();
		}
		aRetourner = result.getSourceAsObjectList(TownSuggest.class);
		for(int i = 0;i<aRetourner.size(); i++){
			System.out.println(aRetourner.get(i).getTownName());
		}
		return aRetourner;
}

public Double[] getLocation(String townName) {
	Double[] aRetourner = new Double[2];
	//Searching
	String query = "{\n" +
			"    \"query\": {\n" +
			"        \"wildcard\" : {\n" +
			"			\"filter\" : {\n" +
				"            \"townName\" : {\n" +               
				"                    \"value\" : \""+townName+"\"\n" +  
				"            }\n"+
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
		e.printStackTrace();
	}
	aRetourner = result.getSourceAsObject(Double[].class);
	return aRetourner;
}

}