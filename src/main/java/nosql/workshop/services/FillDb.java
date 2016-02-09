package nosql.workshop.services;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class FillDb {
	
	public static void fillInstallations() throws IOException{
		String[] labels = {"_id","nom","adresse","numero","voie","lieuDit","codePostal","commune","location","type","point","coordinates","multiCommune","nbPlacesParking","nbPlacesParkingHandicape",
		"DateMiseAJourFiche"};
		fillWithCsv("src/main/resources/batch/csv/installations.csv",labels);
		
	}
	
	public static void fillEquipements() throws IOException{
		Files.lines(Paths.get("src/main/resources/batch/csv/equipements.csv")).forEach(x -> display(x));
	}
	
	public static void fillActivities() throws IOException{
		Files.lines(Paths.get("src/main/resources/batch/csv/activites.csv")).forEach(x -> display(x));
	}
	public static void main(String[] args) throws IOException {
		FillDb.fillInstallations();
	}


	public static void fillWithCsv(String path,String[] labels) throws IOException{


		Stream<String> lines = Files.lines(Paths.get("src/main/resources/batch/csv/installations.csv"));
		lines.forEach(
				x -> insertOneLine(x.split(","), labels)

		);



	}

	public static void insertOneLine(String[] elts,String[] labels){

	}
	
	public static void display(String s){
		System.out.println(s);
	}
	
	public static void writeInstallations(List<String> lines){
		for(int i=1; i<lines.size(); i++){
			DBObject object = new BasicDBObject("type","installation")
							.append("name", "data");
		}				
	}
}
