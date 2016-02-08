package nosql.workshop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class ImportCSV {
	public static void main(String[] args) {
		DB db = new MongoClient().getDB("dbname");

		Jongo jongo = new Jongo(db);
		MongoCollection installations = jongo.getCollection("installations");
		
		String csvFile = "activites.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			
			if ((line = br.readLine()) != null) {
				String[] headers = line.split(cvsSplitBy);
			}
			
			while ((line = br.readLine()) != null) {

			        // use comma as separator
				String[] data = line.split(cvsSplitBy);
				
				
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
