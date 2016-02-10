package nosql.workshop.batch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class CsvToMongoDB {

	
	public static void main(String[] args) {
		CsvToMongoDB.addInstallations();
		CsvToMongoDB.addTown();
	}
	
	public static void addInstallations() {
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
				if(installs[28].length()<10){installs[28]="2014-05-06";}
				DBObject installations = new BasicDBObject().append("_id", installs[1])
						.append("nom", installs[0])
						.append("adresse", new BasicDBObject().append("numero", installs[6])
											.append("voie", installs[7])
											.append("lieuDit", installs[5])
											.append("codePostal", installs[4])
											.append("commune", installs[2]))
						.append("location", new BasicDBObject("type", "Point")
											.append("coordinates", Arrays.asList(Double.valueOf(installs[10]), Double.valueOf(installs[9]))))
						.append("multiCommune", "Oui".equals(installs[16]))
						.append("nbPlacesParking", installs[17].isEmpty() ? null : Integer.valueOf(installs[17]))
						.append("nbPlacesParkingHandicapes", installs[18].isEmpty() ? null : Integer.valueOf(installs[18]))
						.append("dateMiseAJourFiche",installs.length < 29 || installs[28].isEmpty()? null :Date.from(
								LocalDate.parse(installs[28].substring(0, 10)).atStartOfDay(ZoneId.of("UTC")).toInstant()));
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
		
		col.createIndex(new BasicDBObject("location", "2dsphere"));
		
		try {
			cvsSplitBy = ",";
			br = new BufferedReader(new FileReader(csvFileEqu));
			line = br.readLine();
			while ((line = br.readLine()) != null) {
			        // use comma as separator
				String[] equ = line.split(cvsSplitBy);

				equ[0]=equ[0].substring(1, equ[0].length());
				
				BasicDBObject equip = new BasicDBObject().append("_id", equ[4])
						.append("nom", equ[5])
						.append("numero", equ[2])
						.append("type", equ[7])
						.append("famille", equ[9]);
				
				col.update(new BasicDBObject("_id", equ[2]), 
						new BasicDBObject("$push", new BasicDBObject("equipements", equip)));
				
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
				BasicDBObject equipement = new BasicDBObject("equipements", new BasicDBObject("$elemMatch", new BasicDBObject("_id", act[2].trim())));
				BasicDBObject update = new BasicDBObject("$push", new BasicDBObject("equipements.$.activites", act[5]));
				
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
		
	
	public static void addTown(){
		MongoClient mongoclient = new MongoClient();
		DB db = mongoclient.getDB("nosql-workshop");
		DBCollection col = db.getCollection("towns");
		col.drop();
		
		String csvFileInstal = "./src/main/resources/batch/csv/towns_paysdeloire.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFileInstal));
			line = br.readLine();
			while ((line = br.readLine()) != null) {

			        // use comma as separator
				String[] town = line.split(cvsSplitBy);
				
				DBObject towns = new BasicDBObject().append("townName", town[2].substring(1, town[2].length()-1))
						.append("location", Arrays.asList(Double.valueOf(town[7]), Double.valueOf(town[6])));
						
				col.insert(towns);
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
