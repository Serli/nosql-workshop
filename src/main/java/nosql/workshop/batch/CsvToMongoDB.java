package nosql.workshop.batch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;


import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class CsvToMongoDB {

	
	public static void main(String[] args) {
		MongoClient mongoclient = new MongoClient();
		DB db = mongoclient.getDB("nosql-workshop");
		DBCollection col = db.getCollection("installations");
		col.drop();
		
		String csvFileInstal = "./src/main/resources/batch/csv/installations.csv";
		String csvFileEqu = "./src/main/resources/batch/csv/equipements.csv";
		String csvFileactiv = "./src/main/resources/batch/csv/activites.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\",\"";

		try {

			br = new BufferedReader(new FileReader(csvFileInstal));
			line = br.readLine();
			while ((line = br.readLine()) != null) {

			        // use comma as separator
				String[] installs = line.split(cvsSplitBy);

				installs[0]=installs[0].substring(1, installs[0].length());
				System.out.println("Installation [name= " + installs[0] + "]");
				DBObject installations = new BasicDBObject().append("_id", installs[1])
						.append("nom", installs[0])
						.append("Commune", installs[2])
						.append("location", new BasicDBObject("type", "Point")
											.append("coordinates", Arrays.asList(Double.valueOf(installs[10]), Double.valueOf(installs[9]))))
						.append("Code Postal", installs[4])
						.append("Nbequ", installs[26]);
				col.insert(installations);
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
		
		try {
			cvsSplitBy = ",";
			br = new BufferedReader(new FileReader(csvFileEqu));
			line = br.readLine();
			while ((line = br.readLine()) != null) {
			        // use comma as separator
				String[] equ = line.split(cvsSplitBy);

				equ[0]=equ[0].substring(1, equ[0].length());
				System.out.println("Equipement [name= " + equ[5] + "]");
				
				BasicDBObject equip = new BasicDBObject().append("_id", equ[4]).append("nom", equ[5]);
				
				col.update(new BasicDBObject("_id", equ[2]), 
						new BasicDBObject("$push", new BasicDBObject("Equipements", equip)));
				
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
		
		
		
		try {
			cvsSplitBy = "\",\"";
			br = new BufferedReader(new FileReader(csvFileactiv));
			line = br.readLine();
			while ((line = br.readLine()) != null) {
			        // use comma as separator
				String[] act = line.split(cvsSplitBy);

				act[0]=act[0].substring(1, act[0].length());
				
				//BasicDBObject acti = new BasicDBObject().append("_id", act[4]).append("nom", act[5]);
				BasicDBObject equipement = new BasicDBObject("Equipements", new BasicDBObject("$elemMatch", new BasicDBObject("_id", act[2].trim())));
				BasicDBObject update = new BasicDBObject("$push", new BasicDBObject("Equipements.$.Activites", act[5]));
				System.out.println(equipement);
				System.out.println(update);
				
				col.update(equipement, update);
				
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
