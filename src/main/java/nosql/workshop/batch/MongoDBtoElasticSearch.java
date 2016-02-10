package nosql.workshop.batch;

import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.jongo.Find;
import org.jongo.FindOne;
import org.jongo.MongoCollection;

import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.resources.TownResource;
import nosql.workshop.services.MongoDB;


public class MongoDBtoElasticSearch {
	
	public static void main(String[] args) throws UnknownHostException{
		MongoDBtoElasticSearch.addTowns();
		MongoDBtoElasticSearch.addInstallation();
	}

	public static void addInstallation() throws UnknownHostException {

		MongoCollection installations = new MongoDB().getJongo().getCollection("installations");
		int nb = (int) installations.count();
		JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
		try {
			client.execute(new CreateIndex.Builder("installations").build());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for(int i=0; i< nb; i++){
			Installation installation = installations.find().skip(i).as(Installation.class).next();
			installation.set_id(null);
			
			Index index = new Index.Builder(installation).index("installations").type("installation").build();
			try {
				client.execute(index);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		}

	public static void addTowns() throws UnknownHostException{
		MongoCollection towns = new MongoDB().getJongo().getCollection("towns");
		int nb = (int) towns.count();
		JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
		try {
			client.execute(new CreateIndex.Builder("towns").build());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for(int i=0; i< nb; i++){
			TownSuggest town = towns.find().skip(i).as(TownSuggest.class).next();
			town.set_id(null);
			Index index = new Index.Builder(town).index("towns").type("town").build();
			try {
				client.execute(index);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
