package nosql.workshop.batch.mongod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBObject;
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
import nosql.workshop.model.suggest.TownSuggest;

public class ImportTowns {
	
	public static void main(String[] args) throws UnknownHostException {
		ImportTowns obj = new ImportTowns();
		obj.ImpT();
		
	}
	
	public void ImpT(){	
		try (InputStream inputStream = CsvToMongo.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))){
			JestClient client = ESConnectionUtil.createClient("");
//			client.execute(new CreateIndex.Builder("towns").build());
			String line = reader.readLine();
			Index index = null;
//			while ((
					line = reader.readLine();
//					) != null) {
				String[] values = line.split(",");
				List<Double> d = new ArrayList<Double>();
				d.add(Double.parseDouble(values[6]));
				d.add(Double.parseDouble(values[7]));
				TownSuggest ts = new TownSuggest(values[1],d);
				index = new Index.Builder(ts).index("towns").type("town").build();
				client.execute(index);
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
