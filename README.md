
# Big Data Analytics - Analyse de Données Textuelles et Logs Web

## Description
Ce projet regroupe deux rapports de laboratoire réalisés dans le cadre du cours **Big Data et Intelligence Artificielle**. L'objectif est d'analyser des données textuelles et des logs web en utilisant **Spark RDDs** et **Spark DataFrames** afin de comparer leurs performances et optimiser les opérations de traitement des données.

## Introduction
Ce projet se divise en deux grandes parties :
1. **Analyse de données textuelles** : Extraction et traitement d'un corpus de documents en utilisant **Spark RDDs**.
2. **Analyse des logs web** : Comparaison des approches **RDDs** et **DataFrames** pour l'analyse de logs web.

## Préparation des données
- Téléchargement et préparation du corpus de documents et des logs web.
- Nettoyage des données pour faciliter l'analyse.

## Analyse de données textuelles
### Téléchargement et préparation du corpus de documents
- Mise en place de l'environnement Spark et ajout des fichiers de texte.
- Transformation des fichiers en **RDDs** et suppression des caractères non pertinents.

### Comptage des mots
- Tokenisation des textes en mots individuels.
- Filtrage des termes inutiles et comptage des occurrences.

### Recherche des termes fréquents et suppression des mots vides
- Identification des mots les plus fréquents.
- Élimination des mots vides (stop words) pour améliorer l'analyse.

### Création d'un index inversé simple
- Création d'un index listant les documents où chaque mot apparaît.

### Création d'un index inversé étendu
- Ajout du nombre d'occurrences de chaque mot par document.

## Analyse des logs avec Spark RDDs et DataFrames
### Tâche 1: Récupération et affichage des 20 premiers enregistrements
- Transformation et affichage formaté des logs.

### Tâche 2: Nombre total d'enregistrements
- Comparaison entre **RDDs** et **DataFrames** pour compter les enregistrements.

### Tâche 3: Calcul des valeurs min, max et moyenne de la taille des pages
- Comparaison des performances pour extraire ces statistiques.

### Tâche 4: Identification des enregistrements avec la plus grande taille de page
- Deux méthodes explorées avec **RDDs** et une requête SQL pour **DataFrames**.

### Tâche 5: Recherche des pages les plus populaires
- Détermination des pages ayant le plus de vues.

### Tâche 6: Filtrage des enregistrements ayant une taille de page supérieure à la moyenne
- Utilisation de filtres pour extraire les pages les plus volumineuses.

### Tâche 7: Classement des 10 pages les plus populaires et des 5 projets les plus visités
- Comparaison des méthodes **RDDs** et **DataFrames** pour trier et agréger les données.

### Tâche 8: Extraction des mots uniques des titres de page
- Utilisation de transformations avancées pour extraire les mots distincts.

### Tâche 9: Détermination des mots les plus fréquents dans les titres de page
- Calcul de la fréquence des mots présents dans les titres de page.

## Analyse des performances
- Comparaison des temps d'exécution entre **RDDs** et **DataFrames**.
- Explication des écarts de performance observés.
- Discussion sur l'optimisation des requêtes SQL.

## Conclusion
- **DataFrames** sont généralement plus rapides grâce aux optimisations internes de Spark.
- **RDDs** offrent plus de contrôle mais sont souvent moins optimisés.
- Certaines requêtes spécifiques sont plus performantes avec **RDDs**, notamment lorsque l'on manipule directement des transformations complexes.



