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
	/**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;
    
    @Inject
    public SearchService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

	public List<TownSuggest> suggest(String text) {
		//Configuration du client
		JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
		List<TownSuggest> aRetourner = new ArrayList<TownSuggest>();
		try {
			//Indexing
			client.execute(new CreateIndex.Builder("installations").build());
			
			
			List<Installation> source = new ArrayList<Installation>();
			MongoCursor<Installation> cursor = this.installations.find().as(Installation.class);
			while (cursor.hasNext()) {
				source.add(cursor.next());
			}
			
			
			Index index = new Index.Builder(source).index("installations").type("intallation").build();
			client.execute(index);
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
					.addIndex("installations")
					.addType("installation")
					.build();
			JestResult result = client.execute(search);
			
			List<Installation> installationsAfterResult = result.getSourceAsObjectList(Installation.class);
			//for(int i = 0;installationsAfterResult.leng)
				
		} catch (IOException e) {
			e.printStackTrace();
		}
		return aRetourner;
	}

	public Double[] getLocation(String townName) {
		// TODO Auto-generated method stub
		return null;
	}

}
