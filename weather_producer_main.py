import logging
import os
import requests
import json
import paho.mqtt.client as mqtt
import time
from dotenv import load_dotenv




class WeatherProducer:

    def __init__(self, MQTT_BROKER_HOST, MQTT_BROKER_PORT ):

        """
        Initialise le client MQTT pour la connexion au broker.

        :param host: Adresse du broker MQTT.
        :param port: Port du broker MQTT.
        """
        
        self.mqtt_host = MQTT_BROKER_HOST
        self.mqtt_port = MQTT_BROKER_PORT
        self.client = mqtt.Client()
        try:
            self.client.connect(self.mqtt_host, self.mqtt_port)
            logging.info("Connexion au broker MQTT réussi")
        except ConnectionError as ce:
            logging.error("Erreur de connexion au broker MQTT : %s", ce)
        except TimeoutError as te:
            logging.error("Timeout lors de la connexion au broker MQTT : %s", te)
        except Exception as e:
            logging.error("Une erreur s'est produite : %s", e)


    def data_publish(self, data_weather):

        """
        Publie les données météo sur le topic MQTT toute les 1 minutes et les enregistre dans un fichier.

        :param data_weather: Données météo au format JSON.  """


        data = json.dumps(data_weather)
        try:
            self.client.publish("topic/weather", data)
            logging.info("Les données ont été publier sur le topic/weather")

        except Exception as e:
            logging.error("Une erreur lors de la publication des données sur le topic/weather : %s", e)

        try:
            with open("/opt/output/weather.txt", 'a') as f:
                f.write(data +"\n")
                logging.info("Données enregistrer avec succés dans le fichier weather.txt")
        except Exception as e :
            logging.error("Erreur lors de l'écriture des données dans le fichier weather.txt: %s", e)
        time.sleep(60)


class GetDataWeather(WeatherProducer):
    def __init__(self, lat, lon, api_key):
        super().__init__(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
        self.lat = lat
        self.lon = lon
        self.api_key = api_key
        self.url = f'https://api.openweathermap.org/data/2.5/weather?lat={self.lat}&lon={self.lon}&appid={self.api_key}'
        
        

    def get_data(self):
        """
        Récupère les données météo depuis l'API OpenWeatherMap.

        :return: Données météo au format JSON.
        """
        try:
            response = requests.get(self.url)

            if response.status_code == 200 or response.status_code == 201 :
                logging.info("La requête a été traitée avec succès")
                return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error("Une erreur s'est produite lors de la récupération des données :", e)
            return None
        
    def run(self):
        """ Lance la tâche de récupération et de publication des données météo. """
        while True:
            self.data_publish(self.get_data())




# Configuration de la journalisation
logging.basicConfig(level=logging.DEBUG,  #
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
# charger les variables d'environement
load_dotenv()
api_key = os.getenv("API_KEY")

#coordonnées de London
lat = '51.5074'
lon = '-0.1278'

# Nom du service du broker MQTT dans Docker Compose
MQTT_BROKER_HOST = "mosquitto"
MQTT_BROKER_PORT = 1883

# Création de l'instance de weather_producer
WeatherProducer(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
weather_producer = GetDataWeather(lat, lon, api_key)
weather_producer.run()