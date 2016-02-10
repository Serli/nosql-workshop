package nosql.workshop.batch.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.jongo.MongoCollection;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Town;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.MongoDB;

public class MongoToElastic {	
	
	public static void main(String[] args) {
		MongoDB mongoDB = new MongoDB();
		try {
			InstallationService installationService = new InstallationService(mongoDB);
			List<Installation> installations = installationService.getAllInstallations();
			
			//System.out.println(installations.get(0));
			//System.out.println("id = " + installations.get(0).get_id());
			//System.out.println("installation name = " + installations.get(0).getNom());
			//System.out.println("equip = " + installations.get(0).getEquipements().get(0).);

			JestClientFactory factory = new JestClientFactory();
	        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
	                .multiThreaded(true)
	                .build());
	        JestClient client = factory.getObject();

			
			readAndWrite(installations, client);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static void readAndWrite(List<Installation> list, JestClient client){
		
		for(Installation ins:list){
			System.out.println("equipement = " + ins.getEquipements().get(0).getNom());
			Index index = new Index.Builder(ins).index("installations").type("installation").id(ins.get_id()).build();
			try {
				DocumentResult documentResult = client.execute(index);
				System.out.println("succeded = " + documentResult.isSucceeded());
				System.out.println("ID: "+ins.get_id());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}
}
