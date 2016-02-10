package nosql.workshop.batch.elasticsearch;

import com.mongodb.BasicDBObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.suggest.TownSuggest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Samuel Enguehard on 10/02/2016.
 */
public class ImportTowns {


    public static void main(String[] args) {

        // Get the jest client
        JestClient client = ESConnectionUtil.client;

        String csvFile = "src/main/resources/batch/csv/towns_paysdeloire.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {

            br = new BufferedReader(new FileReader(csvFile));

            if ((line = br.readLine()) != null) {
                //String[] headers = line.split(cvsSplitBy);
            }

            String name, id;
            List<Double> loc;
            Index index;
            TownSuggest town;

            while ((line = br.readLine()) != null) {
                String[] data = line.split(cvsSplitBy);
                id = data[0];
                name = data[2].substring(1, data[2].length()-1);
                loc = new ArrayList<>();
                loc.add(Double.parseDouble(data[6])); // X = data[6]
                loc.add(Double.parseDouble(data[7])); // Y = data[7]
                town = new TownSuggest(name, loc);
                index = new Index.Builder(town).index("towns").type("town").id(id).build();
                client.execute(index);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
