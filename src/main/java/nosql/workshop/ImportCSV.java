package nosql.workshop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.mongodb.*;
import org.jongo.Jongo;

public class ImportCSV {
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("nosql-workshop");

		DBCollection installations = db.getCollection("installations");
        DBCollection equipements = db.getCollection("equipements");
		
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

				DBObject equipement = new BasicDBObject("type", "Equipement")
						.append("numero", data[4]) // EquipementID
                        .append("nom", data[5]) // EquNom
                        .append("type", data[7]) // EquipemementTypeLib
                        .append("famille", data[9]); // FamilleFicheLib

				equipements.insert(equipement);
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
