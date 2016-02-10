package nosql.workshop.connection;

import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.PutMapping;
import nosql.workshop.model.Installation;
import org.jongo.Find;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by Vic on 10/02/2016.
 */
public class ESConnection extends ESConnectionUtil {


    private JestClient client;
    int counter = 0;


    public ESConnection(String ip, String port) {
       client = createClient("http://"+ip+":"+port);

    }

    public JestClient getClient(){
        return client;
    }





    public void insertInIndex(MongoCollection collection) {
        System.out.println("=========================");

        try {
            System.out.println("Delete ElasticSearch data if already exists...");
            client.execute(new DeleteIndex.Builder(collection.getName()).build());
            System.out.println("Deleted ! ");
        } catch (IOException e) {
            System.out.println("Nothing to delete, moving on ! ");
        }



        try {
            client.execute(new CreateIndex.Builder(collection.getName()).build());
            Installation i = new Installation();
            i.set_id(null);
            client.execute(new PutMapping.Builder(collection.getName(),collection.getName(),i).build());
        } catch (IOException e) {
            System.out.println("IOException when tried to create index for "+collection.getName());
            e.printStackTrace();

        }

        System.out.println("Only "+collection.count()+" elements in "+collection.getName()+" ! ");
        System.out.print("Inserting in Elastic search");
        collection.find().skip(1).as(Installation.class).forEach(x -> insertOne(x, collection.getName()));
        System.out.println("\n Done ! ");

    }

    private void insertOne(Installation x, String collection) {
        x.set_id(null);
        Index index = new Index.Builder(x).index(collection).type(collection).build();
        try {
            getClient().execute(index);
            if(counter%50==0) System.out.print(".");
            if(counter%2000==0) System.out.println(".");
            if(counter==5000) System.out.print("(oui je sais c'est looooooooong)");
            counter++;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
