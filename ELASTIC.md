Avant le transfert des données de MongoDB dans ElasticSearch, la requête suivante est à exécuter (elle permet d'effectuer des recherches malgré des fautes de frappe ou d'orthographe) :

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
                }
            }
        }
    }
}
```