package nosql.workshop.model;

import org.jongo.marshall.jackson.oid.MongoId;

/**
 * Equipement sportif.
 */
public class Equipement {

    public Equipement() {}

    public Equipement(Integer id) {
        numero = id;
    }

    @MongoId
    private Integer numero;

    public String nom;

    public String type;

    public String famille;

    public String[] activites;

}
