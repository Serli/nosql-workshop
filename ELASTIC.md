# COMMANDES ELASTIC SEARCH A LANCER

## INSTALLATIONS

Avant le transfert des données de MongoDB dans ElasticSearch, la requête suivante est à exécuter :

```
curl -X PUT 'http://localhost:9200/installations' -d '{
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
}'
```

Cette requête permet :
* de préciser le type des coordonnées `geo_point`
* d'autoriser les erreurs de saisies dans les recherches sur certains champs

## TOWNS

Avant d'importer les données des villes depuis le CSV vers ElasticSearch, la requête suivante est à exécuter :

```
curl -X PUT 'http://localhost:9200/towns' -d '{
    "mappings": {
        "town": {
            "properties": {
                "townname_suggest": {
                    "type": "completion",
                    "payloads": true
                }
            }
        }
    }
}'
```
