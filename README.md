# Workshop NoSQL

*Découverte de [MongoDB](http://www.mongodb.org/) et d'[Elasticsearch](http://www.elasticsearch.org/), par la pratique !*


## Prise en main de MongoDB

Quelques rappels avant de démarrer :

* MongoDB est une base de donnnées NoSQL, orientée documents.
* Le format des documents est JSON.
* Les documents sont stockés dans des collections.
* Une base de données MongoDB peut contenir plusieurs collections de documents.
* Il n'est pas possible d'effectuer de jointures entre collections (et ce n'est pas la philosophie).

Dans tous les cas, n'hésitez pas à vous référer à la [documentation officielle](http://docs.mongodb.org/manual/reference/).

### Installation

Téléchargez la dernière version stable de MongoDB sur [mongodb.org/downloads](https://www.mongodb.org/downloads) correspondant à votre système d'exploitation. Ce workshop est basé sur la version 3.2.1 de MongoDB.

Dézippez le bundle dans le dossier de votre choix, par exemple `$HOME/progz/mongodb-3.2.1`.

Les exécutables nécessaires au fonctionnement de MongoDB se trouvent dans le dossier `$HOME/progz/mongodb-3.2.1/bin`.

Pour plus de facilités, vous pouvez ajouter ce dossier à votre `PATH`, afin que les commandes `mongod` et `mongo` soient directement accessibles.
Par exemple sous Linux, ajoutez les lignes suivantes à votre fichier `.profile` :

```bash
# Path to MongoDB binaries
PATH="$HOME/progz/mongodb-3.2.1/bin:$PATH"
export PATH
```

Par défaut, MongoDB stocke ses données dans le dossier `/data/db`. Cela peut être modifié via le paramètre `--dbpath`

Vous pouvez donc créer un dossier spécifique pour stocker les données du workshop, par exemple `$HOME/data/nosql-workshop` :

```bash
mkdir -p "$HOME/data/nosql-workshop"
```

Démarrez MongoDB à l'aide de la commande suivante :

```bash
mongod --dbpath="$HOME/data/nosql-workshop"
```

### Prise en main du shell

MongoDB propose un shell Javascript interactif permettant de se connecter à une instance (démarrée via la commande `mongod`, comme précédemment).

Pour lancer le shell :

```bash
mongo
```

Par défaut, le shell se connecte à l'instance `localhost` sur le port `27017`, sur la base `test` :

```
MongoDB shell version: 2.6.6
connecting to: test
```

Le shell met à disposition un objet Javascript `db` qui permet d'interagir avec la base de données. Par exemple pour obtenir de l'aide :

```javascript
db.help()
```

Pour visualiser les bases disponibles :

```
show dbs
```

Pour changer de base de données, par exemple `workshop` (MongoDB crée automatiquement la base si elle n'existe pas) :

```
use workshop
```

Pour insérer un document dans une collection (la collection est créée automatiquement si elle n'existe pas encore) :

```javascript
db.personnes.insert({ "prenom" : "Jean", "nom" : "DUPONT" })
```

Pour afficher un document :

```javascript
db.personnes.findOne()
```

MongoDB génère automtiquement un identifiant unique pour chaque document, dans l'attribut `_id`. Cet identifiant peut être défini manuellement :

```javascript
db.personnes.insert({ "_id" : "jdupont", "prenom" : "Jean", "nom" : "DUPONT" })
```

Pour voir la liste des collections d'une base de données :

```
show collections
```

### Opérations CRUD

#### Recherche : find()

La méthode `find()` possède deux paramètres (optionnels) :

* le critère de recherche
* la projection (les attributs à retourner)

Par exemple, pour rechercher toutes les personnes se nommant "DUPONT" :

```javascript
	db.personnes.find({ "nom" : "DUPONT" })
```

Si vous ne souhaitez retourner que les noms et prénoms, sans l'identifiant, utilisez une projection (deuxième paramètre de la méthode `find()`) :

```javascript
db.personnes.find({ "nom" : "DUPONT" }, {"_id" : 0, "nom" : 1, "prenom" : 1})
```

#### Insertion : insert()

L'insertion d'un document se fait via la méthode `insert()`, comme vu précédemment lors de la prise en main du Shell.

Le Shell étant un interpréteur Javascript, il est possible d'insérer plusieurs documents à l'aide d'une boucle `for` :

```javascript
for (var i = 1 ; i <= 100 ; i++) {
    db.personnes.insert({ "prenom" : "Prenom" + i, "nom" : "Nom" + i, "age" : (Math.floor(Math.random() * 50) + 20) })
}
```

#### Mise à jour

La mise à jour de documents se fait via la méthode `update()`, qui possède plusieurs paramètres :

* le filtre permettant de sélectionner les documents à mettre à jour
* la requête de mise à jour
* des options (par exemple : `{"multi" : true}` pour mettre à jour tous les documents correspondant au filtre)

Par exemple, pour répartir les personnes dans deux catégories ("Master" pour les plus de 40 ans, "Junior pour les autres") :

```javascript
db.personnes.update({"age" : { "$gte" : 40 }}, {"$set" : {"categorie" : "Master"}}, {"multi" : true})
db.personnes.update({"age" : { "$lt" : 40 }}, {"$set" : {"categorie" : "Junior"}}, {"multi" : true})
```

Remarque : MongoDB a créé automatiquement l'attribut "categorie" qui n'existait pas auparavant !

Remarque 2 : depuis MongoDB 3.2, la commande `updateMany()` peut être utilisée pour éviter d'avoir à préciser `{"multi" : true}` en option de l'instruction `update`. Essayez ! :-)

#### Suppression

La méthode `remove()` permet de supprimer des documents étant donné un filtre :

```javascript
db.personnes.remove({ "nom" : "DUPONT" })
```

Pour supprimer une collection :

```javascript
db.personnes.drop()
```

### Tableaux

Il est possible d'utiliser des tableaux dans les documents. Par exemple, on peut socker les compétences des personnes de la manière suivante :

```javascript
{
    "_id": "jdupont",
    "prenom": "Jean",
    "nom": "DUPONT",
    "competences" : [
        "Java",
        "Javascript",
        "HTML"
    ]
}
```

Pour rechercher les personnes possédant la compétence "Java" :

```javascript
db.personnes.find({ "competences" : "Java" })
```

Pour ajouter une compétence :

```javascript
db.personnes.update({ "_id" : "jdupont" }, {"$push" : {"competences" : "CSS"}})
```

Pour éviter les doublons :

```javascript
db.personnes.update({ "_id" : "jdupont" }, {"$addToSet" : {"competences" : "CSS"}})
```

Pour enlever une compétence :

```javascript
db.personnes.update({ "_id" : "jdupont" }, {"$pull" : {"competences" : "CSS"}})
```

Pour limiter le nombre de compétences à 3 (plus détails [ici](https://docs.mongodb.org/manual/tutorial/limit-number-of-elements-in-updated-array/)) :
```javascript
db.personnes.update(
   { "_id" : "jdupont" },
   {
     $push: {
        "competences": {
           $each: [ "Javascript", "Scala" ],
           $slice: -3
        }
     }
   }
)
```
 

## Prise en main d'Elasticsearch
Avant de démarrer : 

* ElasticSearch est un moteur de recherches distribué
* Il s'appuie sur une base de données NoSQL orientée documents
* Le format des documents est JSON
* ElasticSearch est basé sur la bibliothèque [Lucene](http://lucene.apache.org/)

**A présent rendez-vous sur le [Workshop ElasticSearch](https://github.com/cwoodrow/elasticsearch-workshop)**

## Application Java

L'objectif est de développer une application manipulants des données relatives aux installations sportives de la région Pays de la Loire.

Les données sont issues de [http://data.paysdelaloire.fr](http://data.paysdelaloire.fr).

Trois jeux de données vont particulièrement nous intéresser et sont disponibles dans le projet (format CSV) :

* [Installations](http://data.paysdelaloire.fr/donnees/detail/equipements-sportifs-espaces-et-sites-de-pratiques-en-pays-de-la-loire-fiches-installations)
* [Equipements](http://data.paysdelaloire.fr/donnees/detail/equipements-sportifs-espaces-et-sites-de-pratiques-en-pays-de-la-loire-fiches-equipements)
* [Activités](http://data.paysdelaloire.fr/donnees/detail/equipements-sportifs-espaces-et-sites-de-pratiques-en-pays-de-la-loire-activites-des-fiches-equ)

Des liens existent entre les trois jeux de données :

* une installation possède un ou plusieurs équipements
* une ou plusieurs activités peuvent être pratiquées sur un équipement donné.

![model](assets/model.png)

L'architecture du projet est la suivante : 

![model](assets/ArchitectureWorkshop.png)

* `CsvToMongo` : batch d'import des fichiers CSV dans MongoDB.
* `MongoToElastic` : batch de transfert des données de MongoDB dans Elasticsearch.
* `TownsImporter` : batch d'import des villes des Pays de la Loire dans Elasticsearch.
* `Services` : services métiers proposant les méthodes de lecture et de recherche utilisées par les `Resources`.
* `Resources` : points d'entrée de l'API REST consommée par l'application web.
* `Application web` : ensemble des pages web du workshop (vous n'avez pas à y toucher).

### Import des données dans MongoDB

La première tâche consiste à créer la collection des installations sportives à partir des trois fichiers CSV, en utilisant le driver MongoDB natif Java (package `nosql.workshop.batch.mongodb`).
Il faut donc créer le batch `CsvToMongo`, dont l'exécution se déroule en 3 principales étapes : 

1. lecture du fichier des installations et insertions dans MongoDB.
2. lecture du fichier des équipements et mise à jour des installations précédemment créées.
3. lecture du fichier des activités et mise à jour des équipements des installations.

Exemple d'installation sportive : 

```javascript
{
    "_id": "440390003",
    "nom": "La Pierre Tremblante",
    "adresse": {
        "numero": "",
        "voie": "Chemin des rives",
        "lieuDit": "",
        "codePostal": "44640",
        "commune": "Cheix-en-Retz"
    },
    "location": {
        "type": "Point",
        "coordinates": [
            -1.816274,
            47.181243
        ]
    },
    "multiCommune": false,
    "nbPlacesParking": 0,
    "nbPlacesParkingHandicapes": 0,
    "dateMiseAJourFiche": ISODate("2014-06-18T00:00:00Z"),
    "equipements": [
        {
            "numero": "191989",
            "nom": "La Pierre tremblante",
            "type": "Point d'embarquement et de débarquement isolé",
            "famille": "Site d'activités aquatiques et nautiques",
            "activites": [
                "Canoë de randonnée",
                "Pêche au coup en eau douce"
            ]
        }
    ]
}
```

### Services Java

La seconde tâche consiste à implémenter les `Services` Java (package `nosql.workshop.services`) utilisés par les `Resources`.

L'application web propose une page "API Checkup" permettant de vérifier que les services répondent correctement.

### Recherche full-text

#### Avec MongoDB

On peut par exemple positionner un index de type "text" sur le nom de l'installation et sa commune, en mettant un poids plus important pour la commune :

```javascript
db.installations.ensureIndex(
    {
        "nom" : "text",
        "adresse.commune" : "text"
    },
    {
        "weights" : {
            "nom" : 3,
            "adresse.commune" : 10
        },
        "default_language" : "french"
    }
)
```

Ensuite, on peut par exemple rechercher les "Ports" de la ville de "Carquefou", en triant par pertinance et en ne conservant que les 10 premiers résultats :

```javascript
db.installations.find(
    {
        "$text": {
            "$search": "Port Carquefou",
            "$language" : "french"
        }
    },
    {
        "score": {"$meta": "textScore"}
    }
)
.sort({"score": {"$meta": "textScore"}})
.limit(10)
```

#### Avec Elasticsearch
##### Création du mapping
On crée d'abord le mapping. Dans cette première version, nous nous contenterons d'un mapping simple : 

	curl -XPOST 'http://localhost:9200/installations' -d '{
		"mappings": {
			"installation": {
				"properties": {
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

#### Import des données dans Elasticsearch

Créer le job **MongoDbToElasticsearch** qui a pour objectif de gérer la copie des données de MongoDB à ElasticSearch. Nous ne cherchons pas ici à gérer une mise à jour incrémentale des données.

Nous souhaitons extraire l'ensemble des données de la collection `installations` et les écrire dans l'index `installations` (type `installation`). Afin d'éviter pour le moment des problèmes de conversion de dates, nous filtrerons la propriété `dateMiseAJourFiche` avant l'insersion dans **ElasticSearch**.

#### Villes des Pays de Loire

Créer un job **ImportTowns** qui permet d'importer les villes qui fournies dans le fichier `towns_paysdeloire.csv`,
qui seront accessibles via **TownResource**.

### Recherche Full-Text

Une fois les documents indexés dans ElasticSearch, nous pouvons lancer recherche full text : 

	curl -XPOST 'http://localhost:9200/installations/installation/_search' -d '{
	    "query": {
	        "multi_match": {
	           "query": "Carquefou",
	           "fields": ["_all"]
	        }
	    }
	}'

Vous devez à présent, dans l'application, gérer la recherche à l'aide d'ElasticSearch.
Votre recherche doit tenir compte de la langue (ici le français) afin de proposer les meilleurs
résultats possibles. Par exemple, quand je cherche la chaîne **"fotbal"**, je m'attends à trouver
les terrains de football.

### Suggestion
Vous devez, en utilisant l'API `_suggest` d'ElasticSearch, implémenter la suggestion des villes
en fonction des caractères tapés par l'utilisateur.

### Recherche géographique

#### Avec MongoDB

Tout d'abord, il faut positionner un index géographique de type `2dsphere` sur l'attribut "location" de la collection des installations :

```javascript
db.installations.ensureIndex( { "location" : "2dsphere" } )
```

Ensuite, si l'on souhaite rechercher les installations sportives autour de Carquefou (lat = 47.3, lon = -1.5), dans un rayon de 5km :

```javascript
db.installations.find({ "location" : 
    { $near :
        { $geometry :
            { type : "Point" ,
              coordinates : [ -1.5 , 47.3 ]
            },
            $maxDistance : 5000
        }
    }
})
```

#### Avec Elasticsearch

Pour effectuer la même requête dans ElasticSearch : 

	curl -XPOST 'http://localhost:9200/installations/installation/_search' -d '{
		"query" : {
			"filtered" : {
		        "query" : {
		            "match_all" : {}
		        },
		        "filter" : {
		            "geo_distance" : {
		                "distance" : "5km",
		                "coordinates" : [ -1.5 , 47.3 ]
		            }
			    }
			}
	    }
	}'

