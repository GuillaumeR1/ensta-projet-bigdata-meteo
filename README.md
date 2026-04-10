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
|18 c/ 18|Test 1 : 144.112 s</br>Test 2 : 98.437 s</br>Test 3 : 105.664 s|Test 1 : 2.271 s</br>Test 2 : 2.642 s</br>Test 3 : 2.360 s|Test 1 : 4.455 s</br>Test 2 : 3.398 s</br>Test 3 : 2.640 s|Test 1 : 15.691 s</br>Test 2 : 8.149 s</br>Test 3 : 9.035 s|Test 1 : 4.992 s</br>Test 2 : 2.409 s</br>Test 3 : 2.303 s|

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
|18        |415 MB|Test 1 : 43.649 s</br>Test 2 : 31.781 s</br>Test 3 :38.673 s|Test 1 : 1.416 s</br>Test 2 : 1.528 s</br>Test 3 : 2.287 s|Test 1 : 2.012 s</br>Test 2 : 1.583 s</br>Test 3 : 1.487 s|Test 1 : 10.511 s</br>Test 2 : 10.867 s</br>Test 3 :16.425 s|Test 1 : 2.211 s</br>Test 2 : 2.493 s</br>Test 3 : 3.331 s|
#### Lecture Spark sur Parquet
Temps de génération de fichiers Parquer :
- 4 fichiers en 1 min 58 sec
- 8 fichiers en 1 min 57 sec
- 18 fichiers en 3 min 05 sec

|N fichiers|Taille|Temps lecture I/O|Temps requête 1|Temps requête 2|Temps requête 3|Temps requête 4|
|:---------|:----:|:----------------|:--------------|:--------------|:--------------|:--------------|
|4         |62 MB |Test 1 : 17.581 s<br>Test 2 : 19.428 s<br>Test 3 : 16.338 s|Test 1 : 1.150 s<br>Test 2 : 0.594 s<br>Test 3 : 1.281 s|Test 1 : 1.631 s<br>Test 2 : 1.309 s<br>Test 3 : 1.060 s|Test 1 : 8.013 s<br>Test 2 : 6.075 s<br>Test 3 : 5.469 s|Test 1 : 1.361 s<br>Test 2 : 1.215 s<br>Test 3 : 1.981 s|
|8         |66 MB|Test 1 : 20.447 s</br>Test 2 : 26.286 s</br>Test 3 : 24.194 s|Test 1 : 1.175 s</br>Test 2 : 1.840 s</br>Test 3 : 1.218 s|Test 1 : 2.073 s</br>Test 2 : 0.825 s</br>Test 3 : 0.913 s|Test 1 : 6.834 s</br>Test 2 : 7.234 s</br>Test 3 : 7.042 s|Test 1 : 2.057 s</br>Test 2 : 6.025 s</br>Test 3 : 1.879 s|
|18        |66 MB|Test 1 : 19.390 s</br>Test 2 : 21.840 s</br>Test 3 : 25.947 s|Test 1 : 2.286 s</br>Test 2 : 1.295 s</br>Test 3 : 1.549 s|Test 1 : 1.538 s</br>Test 2 : 0.825 s</br>Test 3 : 1.212 s|Test 1 : 7.962 s</br>Test 2 : 9.437 s</br>Test 3 : 7.276 s|Test 1 : 1.968 s</br>Test 2 : 2.303 s</br>Test 3 : 1.565 s|

Lecture I/O correspond au temps nécessaire pour lire physiquement le fichier depuis le disque, décoder les données, et remplir le cache mémoire Spark. Il s'agit d'un coût unique.</br>
Les temps de requête correspondent au temps de calcule de la requête sur les données en mémoire

## Bilan et limites
Ce projet permet de montrer qu'un pipeline Spark simple peut déjà apporter un gain net sur une volumétrie météo réelle, à condition de bien choisir le format de stockage et de simplifier les données en amont.

### Bilan sur les formats de lecture Spark
- Le CSV est le point d'entrée le plus simple, mais c'est aussi le plus coûteux à exploiter. La lecture est pénalisée par le parsing texte, la décompression des `.csv.gz` et les conversions de type. Il reste utile pour l'ingestion brute, mais peu adapté à des traitements analytiques répétés.
- L'AVRO constitue un premier niveau d'optimisation. On supprime le parsing CSV à chaque lecture et on obtient des temps d'accès plus faibles que sur le brut. En revanche, le gain reste limité pour l'analytique pure, car le format est moins favorable que Parquet aux lectures sélectives et aux agrégations.
- Le PARQUET est le format le plus efficace dans notre cas. Les fichiers sont beaucoup plus compacts, la lecture I/O est plus courte et les requêtes analytiques sont globalement les plus stables. Le format colonnaire est particulièrement pertinent pour des requêtes qui ne mobilisent qu'une partie des colonnes (pour le choix que nous avons fait de nous limiter aux températures pour la définition totale d'une canicule)
- Le partitionnement Parquet par département et année améliore aussi la lecture ciblée : Spark peut éviter de scanner une partie des données quand le filtre correspond aux partitions.
- Le benchmark montre donc une hiérarchie claire pour ce projet : CSV pour l'ingestion, AVRO pour une étape intermédiaire sérialisée et PARQUET pour l'analyse.

### Bilan sur les choix faits dans le projet
- Le projet se concentre sur trois départements. Ce choix réduit le volume traité et permet de construire un prototype lisible, mais il limite la portée nationale des résultats.
- Nous avons retenu un sous-ensemble de variables principalement centré sur la température (`T`, `TN`, `TX`) afin de répondre rapidement aux questions analytiques retenues.
- La détection de canicule repose sur des seuils fixes par département et sur une durée minimale de 3 jours consécutifs. Cette modélisation rend le traitement simple à implémenter et à expliquer.
- Le pipeline de préparation filtre les lignes incomplètes sur `T`, `TN` et `TX`, transforme les types, reconstruit la date et dérive le département à partir de l'identifiant de station. Cela rend les requêtes plus simples et plus rapides par la suite.
- Les tests de performance montrent aussi qu'augmenter le nombre de fichiers ne garantit pas un gain systématique. Il existe un compromis entre parallélisme, coût d'ordonnancement Spark et taille des partitions.

### Limites des considérations métier
- La canicule n'est pas définie uniquement par la température. D'autres variables peuvent jouer un rôle important dans l'impact réel d'un épisode chaud : humidité, vent, rayonnement, durée d'exposition, persistance nocturne, densité urbaine ou vulnérabilité locale.
- Le projet assimile en partie la chaleur extrême à un seuil thermique fixe. Or, en pratique, les seuils de vigilance et les niveaux d'alerte peuvent évoluer selon les référentiels, les territoires et les années.
- Les seuils utilisés sont départementaux, alors que l'exposition réelle peut varier fortement à l'intérieur d'un même département selon l'altitude, la proximité maritime ou l'effet d'îlot de chaleur urbain.
- La logique actuelle retient la station la plus marquante par département pour la plus longue canicule. Cela simplifie le résultat, mais ne produit pas un indicateur agrégé à l'échelle du département entier.
- Le traitement ignore la dimension populationnelle et sanitaire : un épisode chaud détecté météorologiquement n'a pas nécessairement le même impact selon la zone concernée.

### Limites techniques et méthodologiques
- Le benchmark est réalisé sur une machine locale en mode `local[*]`. Les résultats sont donc utiles pour comparer les formats dans ce contexte, mais ils ne représentent pas directement un cluster distribué.
- Les temps mesurés dépendent du cache, du nombre de coeurs disponibles, du nombre de fichiers, de l'état du disque et de l'exécution précédente. Ils doivent être interprétés comme des ordres de grandeur, pas comme des valeurs absolues universelles.
- La phase "lecture I/O" n'est pas strictement la même selon les formats : pour le CSV, elle inclut aussi le parsing, la décompression et une partie du pipeline de transformation, ce qui rend la comparaison utile mais pas parfaitement symétrique.
- Le schéma n'est pas imposé à la lecture CSV dans `Main.runPipeline`, alors qu'un schéma explicite est défini dans le projet. Cela laisse une marge d'amélioration sur la robustesse et la reproductibilité.
- Les lignes incomplètes sont supprimées. Cette décision simplifie les calculs, mais elle peut exclure certaines périodes ou stations et introduire un biais si les valeurs manquantes ne sont pas réparties uniformément.

### Difficultés rencontrées
- La volumétrie initiale et la présence de fichiers compressés rendent les premiers temps de lecture coûteux, en particulier sur CSV.
- Les données météo brutes contiennent beaucoup de colonnes dont seules quelques-unes sont réellement utiles pour les analyses visées ; il a donc fallu faire un travail de sélection et de normalisation.
- Le passage d'une logique de relevés horaires à une logique d'épisodes de canicule sur plusieurs jours demande des agrégations intermédiaires et une gestion explicite des séquences de dates consécutives.
- Le réglage du nombre de partitions/fichiers n'est pas trivial : trop peu de fichiers réduit le parallélisme, trop de fichiers augmente l'overhead Spark.
- La comparaison de performances entre formats demande de bien distinguer coût de génération, coût de lecture, coût de mise en cache et coût de requête, sans quoi l'interprétation des résultats peut être trompeuse.

# Pour aller plus loin
### Améliorations métier
- Faire varier la définition de la canicule au lieu d'utiliser un seuil fixe unique. Une première extension naturelle consiste à intégrer des seuils annuels ou saisonniers ou à charger des seuils externes versionnés par année et par territoire.
- Intégrer d'autres variables explicatives que la seule température : humidité, vent, pression, rayonnement, durée des nuits chaudes ou indicateurs composites de chaleur ressentie.
- Construire un indice plus réaliste de canicule ou de stress thermique, par exemple à partir d'une combinaison `TX`, `TN`, persistance sur plusieurs jours et humidité.
- Passer d'une logique par station à une logique réellement départementale, en agrégeant les stations d'un même territoire avec une stratégie explicite.
- Mieux distinguer chaleur ponctuelle, vague de chaleur et canicule selon des règles métier paramétrables.

### Améliorations techniques
- Utiliser systématiquement un schéma Spark explicite dès la lecture CSV pour réduire l'ambiguïté sur les types et améliorer la robustesse du pipeline.
- Externaliser les seuils de canicule dans un fichier de configuration ou une table de référence plutôt que de les coder en dur dans `ParquetAnalytics`.
- Ajouter des tests automatiques sur la qualité des données : taux de valeurs manquantes, doublons, distribution par station, continuité temporelle.
- Étendre les benchmarks avec des moyennes, médianes et écarts-types, afin de mieux quantifier la variabilité observée entre essais.
- Tester d'autres stratégies de partitionnement et de `shuffle partitions`, voire un partitionnement plus fin selon les requêtes réellement exécutées.
- Comparer la lecture "cold" et "warm" cache de manière plus formelle pour séparer l'effet du stockage de l'effet mémoire.

### Perspectives possibles
- Étendre l'analyse à davantage de départements, voire à tout le territoire, afin de comparer littoral, zones urbaines, zones rurales et relief.
- Ajouter une dimension cartographique ou temporelle interactive pour visualiser l'évolution des épisodes chauds.
- Croiser les résultats météo avec d'autres sources ouvertes, par exemple des données démographiques, sanitaires ou énergétiques, afin d'évaluer l'impact des canicules au-delà de la seule mesure physique.
- Étudier l'évolution des épisodes extrêmes à différentes échelles temporelles : année, décennie, saison, voire avant/après certaines ruptures climatiques observables.

# Reproduction
## Dépendances
> Java 17</br>
> Spark 3.5.x

## Setup
1. Télécharger le repot git
2. Créer un dossier tmp/data, et y télécharger les fichiers data sur le site donnée au début
3. Exécuter les classes TestAvro.java et TestParquet.java
4. Eécuter TestParquetAnalytics.java ou TestPerformance.java
