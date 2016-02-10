package nosql.workshop.batch.mongodb;

import io.searchbox.annotations.JestId;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.services.MongoDB;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by Jorpheus on 09/02/2016.
 */
public class MongoDbToElasticSearch {

    public static final String ES_INDEX = "nosql-workshop";
    public static final String COLLECTION_NAME = "installations";
    private final MongoCollection installations;
    private final Logger LOGGER = Logger.getLogger(getClass().getName());

    public MongoDbToElasticSearch() throws IOException {

        MongoDB mongoDB = new MongoDB();
        installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);

        List<Installation> myList = new ArrayList<>();
        MongoCursor<Installation> all = installations.find().as(Installation.class);
        while(all.hasNext()) {
            myList.add(all.next());
        }


        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .readTimeout(999999999)
                .build());
        JestClient jestClient = factory.getObject();


        Bulk.Builder bulkIndexBuilder = new Bulk.Builder();
        for(Installation i : myList){
            bulkIndexBuilder.addAction(new Index.Builder(new InstallationJest(i)).index(ES_INDEX).type("installation").build());
        }


        try {
            BulkResult bulkResult = jestClient.execute(bulkIndexBuilder.build());
            for (BulkResult.BulkResultItem bulkResultItem : bulkResult.getFailedItems()) {
                LOGGER.severe("Error when processing the bulk: "+ bulkResultItem.error);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }




    }

    public static void main(String[] args) throws IOException {
        new MongoDbToElasticSearch();
    }

    private class InstallationJest {
        @JestId
        private final String id;
        private String nom;
        private Installation.Adresse adresse;
        private Installation.Location location;
        private boolean multiCommune;
        private int nbPlacesParking;
        private int nbPlacesParkingHandicapes;
        private Date dateMiseAJourFiche;
        private List<Equipement> equipements;

        public String getId() {
            return id;
        }

        public String getNom() {
            return nom;
        }

        public void setNom(String nom) {
            this.nom = nom;
        }

        public Installation.Adresse getAdresse() {
            return adresse;
        }

        public void setAdresse(Installation.Adresse adresse) {
            this.adresse = adresse;
        }

        public Installation.Location getLocation() {
            return location;
        }

        public void setLocation(Installation.Location location) {
            this.location = location;
        }

        public boolean isMultiCommune() {
            return multiCommune;
        }

        public void setMultiCommune(boolean multiCommune) {
            this.multiCommune = multiCommune;
        }

        public int getNbPlacesParking() {
            return nbPlacesParking;
        }

        public void setNbPlacesParking(int nbPlacesParking) {
            this.nbPlacesParking = nbPlacesParking;
        }

        public int getNbPlacesParkingHandicapes() {
            return nbPlacesParkingHandicapes;
        }

        public void setNbPlacesParkingHandicapes(int nbPlacesParkingHandicapes) {
            this.nbPlacesParkingHandicapes = nbPlacesParkingHandicapes;
        }

        public Date getDateMiseAJourFiche() {
            return dateMiseAJourFiche;
        }

        public void setDateMiseAJourFiche(Date dateMiseAJourFiche) {
            this.dateMiseAJourFiche = dateMiseAJourFiche;
        }

        public List<Equipement> getEquipements() {
            return equipements;
        }

        public void setEquipements(List<Equipement> equipements) {
            this.equipements = equipements;
        }

        public InstallationJest(Installation i) {
            this.id = i.get_id();
            this.nom = i.getNom();
            this.adresse = i.getAdresse();
            this.location = i.getLocation();
            this.equipements = i.getEquipements();
            this.nbPlacesParking = i.getNbPlacesParking();
            this.nbPlacesParkingHandicapes = i.getNbPlacesParkingHandicapes();
        }
    }
}
