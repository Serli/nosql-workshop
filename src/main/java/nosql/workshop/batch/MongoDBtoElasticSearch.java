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
import nosql.workshop.services.MongoDB;


public class MongoDBtoElasticSearch {

	public static void main(String[] args) throws UnknownHostException {

		MongoCollection installations = new MongoDB().getJongo().getCollection("installations");
		int nb = (int) installations.count();
		for(int i=0; i< nb; i++){
			Installation installation = installations.find().skip(i).as(Installation.class).next();
			installation.set_id(null);
			JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
			
			Index index = new Index.Builder(installation).index("installations").type("installation").build();
			try {
				client.execute(new CreateIndex.Builder("installations").build());
				client.execute(index);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		}


}
