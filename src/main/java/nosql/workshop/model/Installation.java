package nosql.workshop.model;


import org.jongo.marshall.jackson.oid.MongoId;

/**
 * Installation sportive.
 */
public class Installation {


    public Installation() {}


    public Installation(String id) {
        _id = id;
    }

    @MongoId
    private String _id;

    public String nom;

    public Adresse adresse;

    public Location location;

    public Boolean multiCommune;

    public Integer nbPlacesParking;

    public Integer nbPlacesParkingHandicapes;

    public String dateMiseAJourFiche;

    public Equipement[] equipements;
}
