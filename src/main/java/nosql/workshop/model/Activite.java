package nosql.workshop.model;

import org.jongo.marshall.jackson.oid.MongoId;

/**
 * Created by Samuel Enguehard on 09/02/2016.
 */
public class Activite {

    @MongoId
    private String _id;
    private String nom;
    private String code;
    private String niveau;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getNiveau() {
        return niveau;
    }

    public void setNiveau(String niveau) {
        this.niveau = niveau;
    }
}
