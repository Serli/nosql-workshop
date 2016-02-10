package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import nosql.workshop.model.Town;

import static org.elasticsearch.node.NodeBuilder.*;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class TownsImporter {
	
	
	public static void main(String[] args) {
		//Node node = nodeBuilder().node();
		/*Client client = new TransportClient().addTransportAddress(
				new InetSocketTransportAddress("host", 9300));*/
		JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
		//Client client = node.client();
		
		write(client);
		//client.shutdownClient();
		//node.close();
				
	}
	
	public static void write(JestClient client){
		try (InputStream inputStream = TownsImporter.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			reader.lines()
			.skip(1)
			.filter(line -> line.length() > 0)
			.map(line -> line.split(","))
			.forEach(columns -> {
				String objectID = columns[0];
				System.out.println("Object ID : "+objectID);
				String townName = columns[1].matches("\".*\"")?columns[1].substring(1,columns[1].length()-1):columns[1];
				System.out.println("town name : "+townName);
				String townNameSuggest = columns[2].matches("\".*\"")?columns[2].substring(1,columns[2].length()-1):columns[2];
				String postcode = columns[3].matches("\".*\"")?columns[3].substring(1,columns[3].length()-1):columns[3];
				String pays = columns[4].matches("\".*\"")?columns[4].substring(1,columns[4].length()-1):columns[4];
				String region = columns[5].matches("\".*\"")?columns[5].substring(1,columns[5].length()-1):columns[5];
				double x = Double.parseDouble(columns[6].trim());
				double y = Double.parseDouble(columns[7].trim());
				
				//XContentBuilder builder;
				try {
//					builder = jsonBuilder()
//							 .startObject()
//						        .field("objectID", objectID)
//						        .field("townName", townName)
//						        .field("townNameSuggest", townNameSuggest)
//						        .field("postcode", postcode)
//						        .field("pays", pays)
//						        .field("region", region)
//						        .field("x", x)
//						        .field("y", y)
//						    .endObject();
					
					/*IndexResponse response = client.prepareIndex("towns","town", objectID)
						    .setSource(builder)
						    .execute()
						    .actionGet();
					System.out.println("Is created : "+response.isCreated());
					System.out.println("ID : "+objectID);*/

					Town source = new Town();
					source.setObjectID(objectID);
					source.setTownName(townName);
					source.setTownNameSuggest(townNameSuggest);
					source.setPostcode(postcode);
					source.setPays(pays);
					source.setRegion(region);
					source.setX(x+"");
					source.setY(y+"");
					
					Index index = new Index.Builder(source).index("towns").type("town").id(objectID).build();
					client.execute(index);
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			});
			
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}


	}

	
}
