package nosql.workshop.model;

import org.jongo.marshall.jackson.oid.MongoId;

import java.util.List;

/**
 * Equipement sportif.
 */
public class Equipement {

    @MongoId
    private String _id;
    private String numero;
    private String nom;
    private String type;
    private String famille;
    private List<String> activites;

    public String get_id() { return this._id; }

    public void set_id(String _id) { this._id = _id; }

    public String getNumero() {
        return this.numero;
    }

    public void setNumero(String numero) {
        this.numero = numero;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFamille() {
        return famille;
    }

    public void setFamille(String famille) {
        this.famille = famille;
    }

    public List<String> getActivites() {
        return activites;
    }

    public void setActivites(List<String> activites) {
        this.activites = activites;
    }
}
