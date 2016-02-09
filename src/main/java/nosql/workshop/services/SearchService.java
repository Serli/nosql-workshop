package nosql.workshop.services;

import net.codestory.http.Context;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;

import java.util.List;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

   public List<TownSuggest> suggest(String text) {
       return null;
   }

    public List<Installation> search(Context context) {
        return null;

    }

    public List<Installation> geosearch(Context context) {
        return null;

    }
}
