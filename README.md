# Projet Pipeline d'Acquisition de Données Météorologiques en Temps Réel

L'objectif de ce projet est de construire un pipeline d'acquisition de données météorologiques en temps réel et de les enregistrer dans un fichier CSV. Le pipeline utilisera une architecture producteur-consommateur avec deux conteneurs Docker pour assurer la séparation des tâches.

## Producteur de Données Météorologiques

Le producteur est responsable de récupérer les données météorologiques en temps réel via l'API OpenWeather. Les données sont ensuite publiées sur un broker MQTT (Message Queuing Telemetry Transport).

## Consommateur de Données Météorologiques

Le consommateur se connecte au broker MQTT, récupère les données publiées par le producteur et les traite. Les données sont filtrées et transformées en un format adapté pour être stockées dans un fichier CSV.

## Stockage des Données

Les données météorologiques filtrées sont stockées dans un fichier CSV. Le fichier CSV est créé ou mis à jour à chaque nouvelle donnée reçue.

## Instructions d'exécution

1. Clonez le projet à partir du référentiel Git.

2. Assurez-vous que Docker et Docker Compose sont installés sur votre système.

3. Placez votre clé API OpenWeather dans le fichier ".env" situé à la racine du projet.
   exemple :  API_KEY=xxxxxxxxx
   Remplacez `xxxxxxxxx` par votre clé API OpenWeather.

4. Accédez au répertoire du projet et exécutez la commande suivante pour créer et démarrer les conteneurs :

```bash
docker-compose up -d --build



