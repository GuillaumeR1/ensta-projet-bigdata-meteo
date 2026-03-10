# Présentation
Ce projet vis à l'optimisation de l'interrogation d'une base de donnée météorologique issue de mété France pour permettre une analyse fine des épisodes de canacile en France. <br/>
Il se concentre sur les départements :
- Gironde (33)
- Bouches du Rhône (13)
- Rhône (69)<br/>

entre 1970 et 2024. <br/>

> Source dataset : https://meteo.data.gouv.fr/datasets/6569b4473bedf2e7abad3b72

### Problématique : comment analyser des séries temporelles de température en France afin d’identifier des tendances, anomalies et canicules suivant la double échelle régionale et temporelle ?

## Justification
La Gironde est un département couramment concerné par des forte vague de chaleur qui impacte directement l'économique locale en raison de la forte activié viticole et la protection des population en raison des feux de forêt. <br/>
Les Bouches du Rhône connait des été très chaud, sensible au canicule avec une forte densité de population en particulier l'été en raison de l'activité touristique autour de la méditérannée. <br/>
Le Rhône, dont la métropole de Lyon, fortement urbanisé représente un îlot de chaleur avec également une forte densité de population.

# Mise en oeuvre

## Périmètre
Dans le cadre de ce projet, nous nous intéressons à répondre aux questions types suivantes : <br/>
- Retrouver tous les relevés horaires chauds où T >= 35°C 
- Compter les jours de forte chaleur par département et par année
- Trouver les périodes de canicules les plus long par département 
- Top 10 des journées les plus chaudes <br/>

Canicule (déf) : épisode de forte chaleur sur une période d'au moins 3 jours et dont les températures dépassent les seuils suivants :
|Département|Seuil journée|Seuil nocturne|
|:---------:|:------------:|:------------:|
|13         |35°C          |24°C          |
|33         |35°C          |21°C          |
|69         |34°C          |20°C          |

Le dataset de météo France présente beaucoup de donnée, dont certaines non pertinente dans notre cas. Les colonnes les plus importantes sont :
- ```NOM_USUEL``` pour l'identification de la station
- ```AAAAMMJJHH``` pour la donnée temporel
- ```LAT```, ```LON``` et ```ALTI``` pour la donnée géographique
- ```T```, ```TN```, et ```TX``` pour les informations de température 
- Le département contenu dans le nom du fichier </br>

## Principe d'optimisation
1. Définir un schéma explicite : 
Évite l’inférence répétée, stabilise les types, accélère la lecture.</br>
2. Structuration avec AVRO : 
Avro apporte un format sérialisé, typé, adapté à l’échange de données.</br>
3. Format analytique avec PARQUET : 
Parquet est orienté colonnes, donc idéal pour des requêtes qui lisent peu de colonnes sur un gros dataset.</br>
4. Partitionnement par département / année / mois : 
Réduit le nombre de fichiers lus.</br>

## Développement
### Niveau 1
Préparation DataSet burt pour un dataset propre et exploitable par SPARK
.csv -> lecture SPARK et schéma -> Formatage de donnée -> Data prêt pour exploitation

### Niveu 2
Optimisation via avro pour optimiser la taille de dataset, le temps de lecture et la structure des données.

### Niveau 3

### Niveau 4
### PIPELINE : .csv -> lecture SPARK et schéma -> Formatage de donnée -> convertion AVRO ->
