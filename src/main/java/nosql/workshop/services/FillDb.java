package nosql.workshop.services;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FillDb {
	
	public static void fillInstallations() throws IOException{
		Files.lines(Paths.get("src/main/resources/batch/csv/installations.csv")).forEach(x -> display(x));
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
	
	public static void display(String s){
		System.out.println(s);
	}
}
