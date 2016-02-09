package nosql.workshop.services;

import com.google.inject.Singleton;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;

import java.util.List;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {

    public List<Installation> getAllInstallations(){
        return null;
    }

    public Installation getInstallation(Double numero){
        return null;
    }

    public List<Installation> searchInstallations(){
        return null;
    }

    public Installation getRandomInstallation(){
        return null;
    }

    public List<Installation> getInstallationByGeoSearch(){
        return null;
    }

    public InstallationsStats getStats(){
        return null;
    }
}
