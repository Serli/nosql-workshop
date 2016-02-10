package nosql.workshop.batch.mongod;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.google.inject.Inject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.services.MongoDB;

public class MongoDbToElasticsearch {
	
	public static void main(String[] args) throws UnknownHostException {
		MongoDbToElasticsearch obj = new MongoDbToElasticsearch();
		obj.MtoE();
		
	}

	public void MtoE(){
		try {
			MongoClient mongo = new MongoClient();
			DB db = mongo.getDB("nosql-workshop");
			DBCollection collection = db.getCollection("installations");
					
			JestClient client = ESConnectionUtil.createClient("http://localhost:9200");
			client.execute(new CreateIndex.Builder("installations").build());
			Index index = null;
			DBCursor cursor = collection.find();
			cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
			while (cursor.hasNext()) {
				DBObject dbObject = cursor.next();
				dbObject.put("id",dbObject.get("_id"));
				dbObject.removeField("_id");
				dbObject.removeField("dateMiseAJourFiche");
				dbObject.removeField("location");
				index = new Index.Builder(dbObject.toString()).index("installations").type("intallation").build();
				client.execute(index);
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
