# COMMANDES ELASTIC SEARCH A LANCER

Avant le transfert des données de MongoDB dans ElasticSearch, la requête suivante est à exécuter :

```
curl -X PUT http://localhost:9200/installations
{
    "mappings": {
        "installation": {
            "properties": {
                "adresse": {
                    "properties": {
                        "voie": {
                            "type": "string",
                            "analyzer": "french"
                        }
                    }
                },
                "equipements": {
                    "properties": {
                        "activites": {
                            "type": "string",
                            "analyzer": "french"
                        },
                        "famille": {
                            "type": "string",
                            "analyzer": "french"
                        },
                        "nom": {
                            "type": "string",
                            "analyzer": "french"
                        },
                        "type": {
                            "type": "string",
                            "analyzer": "french"
                        }
                    }
                },
                "location": {
                    "properties": {
                        "coordinates": {
                            "type": "geo_point"
                        }
                    }
                }
            }
        }
    }
}
```

Cette requête permet :
* de préciser le type des coordonnées `geo_point`
* d'autoriser les erreurs de saisies dans les recherches sur certains champs