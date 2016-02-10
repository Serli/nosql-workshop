package nosql.workshop.batch.mongod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
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
			String line = reader.readLine();
			Index index = null;
			while ((line = reader.readLine()) != null) {
				String[] values = line.split(",");
				List<Double> d = new ArrayList<Double>();
				d.add(Double.parseDouble(values[6]));
				d.add(Double.parseDouble(values[7]));
				TownSuggest ts = new TownSuggest(values[1].substring(1, values[1].length()-1),d);
				String json = new Gson().toJson(ts);
				index = new Index.Builder(ts).index("towns").type("town").build();
				client.execute(index);
							}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}