package nosql.workshop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

import com.mongodb.*;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class ImportCSV {
	
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("nosql-workshop");

		DBCollection installations = db.getCollection("installations");
		DBCollection equipements = db.getCollection("equipements");
        DBCollection activites = db.getCollection("activites");
		
		//importInstallation(installations);
		//importEquipement(equipements);
        importActivite(activites);
		
	}
	
	public static void importInstallation(DBCollection installations) {


		String csvFile = "src/main/resources/batch/csv/installations.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\"";

		try {

			br = new BufferedReader(new FileReader(csvFile));

			if ((line = br.readLine()) != null) {
				//String[] headers = line.split(cvsSplitBy);
			}
			DBObject inst = new BasicDBObject();
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] data = line.split(cvsSplitBy);
				//System.out.println(data.length);
				//Nom usuel de l'installation0,"NumÃ©ro de l'installation"1,"Nom de la commune"2,"Code INSEE"3,"Code postal"4,"Nom du lieu dit"5,"Numero de la voie"6,"Nom de la voie"7,"location"8,"Longitude"9,"Latitude"10,"Aucun amÃ©nagement d'accessibilitÃ©"11,"AccessibilitÃ© handicapÃ©s Ã  mobilitÃ© rÃ©duite"12,"AccessibilitÃ© handicapÃ©s sensoriels"13,"Emprise fonciÃ¨re en m2"14,"GardiennÃ©e avec ou sans logement de gardien"15,"Multi commune"16,"Nombre total de place de parking"17,"Nombre total de place de parking handicapÃ©s"18,"Installation particuliÃ¨re"19,"Desserte mÃ©tro"19,"Desserte bus"20,"Desserte Tram"21,"Desserte train"22,"Desserte bateau"23,"Desserte autre"24,"Nombre total d'Ã©quipements sportifs"25,"Nombre total de fiches Ã©quipements"26,"Date de mise Ã  jour de la fiche installation"27
				inst = new BasicDBObject()
						.append("_id", data[3])
						.append("nom", data[1])
						.append("adresse", new BasicDBObject()
								.append("numero", data[13])
								.append("voie", data[15])
								.append("lieuDit", data[11])
								.append("codePostal", data[9])
								.append("commune", data[5])
								)
						.append("location", new BasicDBObject()
								.append("type", "point")
								.append("coordinates",  Arrays.asList(Double.parseDouble(data[19]), Double.parseDouble(data[21])))
						)
						.append("multiCommune", "Oui".equals((data[35])))
						.append("nbPlacesParking", data[35].isEmpty() ? null : Integer.valueOf(data[35]))
						.append("nbPlacesParkingHandicapes", data[37].isEmpty() ? null : Integer.valueOf(data[37]))
						.append(
								"dateMiseAJourFiche",
								data.length < 58 || data[57].isEmpty()
								? null :
									Date.from(
											LocalDate.parse(data[57].substring(0, 10))
											.atStartOfDay(ZoneId.of("UTC"))
											.toInstant()
											)
								);

				installations.insert(inst);


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
	

	public static void importEquipement(DBCollection equipements) {
        String csvFile = "src/main/resources/batch/csv/equipements.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {

            br = new BufferedReader(new FileReader(csvFile));

            if ((line = br.readLine()) != null) {
                //String[] headers = line.split(cvsSplitBy);
            }
            DBObject equipement = new BasicDBObject();

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] data = line.split(cvsSplitBy);

                equipement = new BasicDBObject("type", "Equipement")
                        .append("_id", data[4]) // EquipementId
                        .append("numero", data[2]) // InsNumeroInstall
                        .append("nom", data[5]) // EquNom
                        .append("type", data[7]) // EquipemementTypeLib
                        .append("famille", data[9]); // FamilleFicheLib


            }
            equipements.insert(equipement);

            // TODO update installation with equipementID

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

    public static void importActivite(DBCollection activites) {

        String csvFile = "src/main/resources/batch/csv/activites.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "\",\"";

        try {

            br = new BufferedReader(new FileReader(csvFile));

            if ((line = br.readLine()) != null) {
                //String[] headers = line.split(cvsSplitBy);
            }
            DBObject activite = new BasicDBObject();

            while ((line = br.readLine()) != null) {
                
                String[] data = line.split(cvsSplitBy);

                equipement = new BasicDBObject("type", "Equipement")
                        .append("numero", getCSVData()) // EquipementID
                        .append("nom", data[5]) // EquNom
                        .append("type", data[7]) // EquipemementTypeLib
                        .append("famille", data[9]); // FamilleFicheLib


            }
            equipements.insert(equipement);
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

    private static String getCSVData(int column, String[] line){
        return line[column*2];
    }

}
