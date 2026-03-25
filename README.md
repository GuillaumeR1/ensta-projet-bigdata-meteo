# Présentation
Ce projet vise à l'optimisation de l'interrogation d'une base de donnée météorologique issue de mété France pour permettre une analyse fine des épisodes de canacile en France. <br/>
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
- Comment évolue la fréquence des températures >= 35°C (températures caniculaires) au fil des décennies ?
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
1. Définir un schéma explicite pour SPARK limité aux variables utiles. Les données sont également converties vers des types adaptés afin de faciliter les traitements analytiques.</br>
2. Les données sont converties en AVRO qui ne nécessite pas de parsing à chaque lecture et réduit le temps d'accès. Amélioration des performance de requête via Spark.</br>
3. Données stockées en PARQUET optimisé pour l'analytique. Les fichiers sont partitionné en départements et années permettant à SPARK de lire uniquement les fichiers nécessaires.</br>

## Benchmark de comparaison des performances

#### Lecture Spark sur csv
Dans notre cas d'étude, il y a 18 fichiers .csv.gz pour 566 MB
|        |Temps lecture I/O|Temps requête 1|Temps requête 2|Temps requête 3|Temps requête 4|
|:------:|:----------------|:--------------|:--------------|:--------------|:--------------|
|18 c/ 4 |Test 1 : 96.415 s<br>Test 2 : 113.859 s<br>Test 3 : 103.753 s|Test 1 : 1.794 s<br>Test 2 : 2.316 s<br>Test 3 : 1.561 s|Test 1 : 2.999 s<br>Test 2 : 4.852 s<br>Test 3 : 3.759 s|Test 1 : 8.882 s<br>Test 2 : 12.133 s<br>Test 3 : 9.644 s|Test 1 : 3.210 s<br>Test 2 : 2.485 s<br>Test 3 : 2.381 s|
|18 c/ 8 |Test 1 : 112.615 s<br>Test 2 : 116.773 s<br>Test 3 : 118.933 s|Test 1 : 2.660 s<br>Test 2 : 1.385 s<br>Test 3 : 2.106 s|Test 1 : 3.197 s<br>Test 2 : 3.766 s<br>Test 3 : 3.431 s|Test 1 : 7.253 s<br>Test 2 : 12.707 s<br>Test 3 : 8.829 s|Test 1 : 3.043 s<br>Test 2 : 2.923 s<br>Test 3 : 2.831 s|
|18 c/ 18|Test 1 : <br>Test 2 : <br>Test 3 : <br>|Test 1 : <br>Test 2 : <br>Test 3 : <br>|Test 1 : <br>Test 2 : <br>Test 3 : <br>|Test 1 : <br>Test 2 : <br>Test 3 : <br>|Test 1 : <br>Test 2 : <br>Test 3 : <br>|

En plus du format d'optimisation, chaque test sont effectués en modifiant le paramètre du nombre de fichiers Avro et Parquet créés.</br>
- 18, pour avoir le même nombre de fichiers que dans le dataset inital
- 4 et 8 déterminer en fonction du nombre de coeur disponible sur la machine de teste

#### Lecture Spark sur Avro
Temps de génération de fichiers Avro :
- 4 fichiers en 3 min 26 sec
- 8 fichiers en 2 min 24 sec
- 18 fichiers en 3 min

|N fichiers|Taille|Temps lecture I/O|Temps requête 1|Temps requête 2|Temps requête 3|Temps requête 4|
|:---------|:----:|:----------------|:--------------|:--------------|:--------------|:--------------|
|4         |473 MB|Test 1 : 31.048 s <br>Test 2 : 54.653 s<br>Test 3 : 33.826 s|Test 1 : 1.824 s<br>Test 2 : 1.639 s<br>Test 3 : 0.722 s|Test 1 : 1.298 s<br>Test 2 : 2.593 s<br>Test 3 : 1.473 s|Test 1 : 9.976 s<br>Test 2 : 14.769 s<br>Test 3 : 8.711 s|Test 1 : 2.190 s<br>Test 2 : 2.845 s<br>Test 3 : 2.508 s|
|8         |411 MB|Test 1 : 33.240 s</br>Test 2 : 61.849 s</br>Test 3 : 36.547 s|Test 1 : 2.288 s</br>Test 2 : 1.592 s</br>Test 3 : 1.351 s|Test 1 : 2.056 s</br>Test 2 : 1.794 s</br>Test 3 : 2.624 s|Test 1 : 12.281 s</br>Test 2 : 11.776 s</br>Test 3 : 13.982 s|Test 1 : 1.929 s</br>Test 2 : 2.752 s</br>Test 3 : 4.691 s|
|18        |415 MB|||||
#### Lecture Spark sur Parquet
Temps de génération de fichiers Parquer :
- 4 fichiers en 1 min 58 sec
- 8 fichiers en 1 min 57 sec
- 18 fichiers en 3 min 05 sec

|N fichiers|Taille|Temps lecture I/O|Temps requête 1|Temps requête 2|Temps requête 3|Temps requête 4|
|:---------|:----:|:----------------|:--------------|:--------------|:--------------|:--------------|
|4         |62 MB |Test 1 : 17.581 s<br>Test 2 : 19.428 s<br>Test 3 : 16.338 s|Test 1 : 1.150 s<br>Test 2 : 0.594 s<br>Test 3 : 1.281 s|Test 1 : 1.631 s<br>Test 2 : 1.309 s<br>Test 3 : 1.060 s|Test 1 : 8.013 s<br>Test 2 : 6.075 s<br>Test 3 : 5.469 s|Test 1 : 1.361 s<br>Test 2 : 1.215 s<br>Test 3 : 1.981 s|
|8         |66 MB|Test 1 : 20.447 s</br>Test 2 : 26.286 s</br>Test 3 : 24.194 s|Test 1 : 1.175 s</br>Test 2 : 1.840 s</br>Test 3 : 1.218 s|Test 1 : 2.073 s</br>Test 2 : 0.825 s</br>Test 3 : 0.913 s|Test 1 : 6.834 s</br>Test 2 : 7.234 s</br>Test 3 : 7.042 s|Test 1 : 2.057 s</br>Test 2 : 6.025 s</br>Test 3 : 1.879 s|
|18        |66 MB|||||

## Bilan et limites
> à faire

# Pour aller plus loin
> à faire
