import paho.mqtt.client as mqtt
import logging
import json
import pandas as pd




class WeatherConsumer:

    """
        Initialise le consommateur de données météo MQTT.

        :param MQTT_BROKER_HOST: Adresse du broker MQTT.
        :param MQTT_BROKER_PORT: Port du broker MQTT.
    """
    def __init__(self,MQTT_BROKER_HOST,MQTT_BROKER_PORT  ):
        self.mqtt_host = MQTT_BROKER_HOST
        self.mqtt_port = MQTT_BROKER_PORT
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe('topic/weather')
        logging.info("Connexion au topic réussie")

    


    def on_message(self, client, userdata, msg):
        """
        Méthode de rappel appelée lorsqu'un message est reçu sur le topic MQTT.
        Traite et enregistre les données météo dans un fichier CSV.

        :param client: Client MQTT.
        :param userdata: Données utilisateur.
        :param msg: Message MQTT.
        """
        
        try:
            data_weather = msg.payload.decode()
            data_weather = json.loads(data_weather)
            logging.info("Données récupérées et converties au format JSON")

        except json.JSONDecodeError as e:
            logging.error("Erreur lors de la conversion des données JSON : %s", e)
        except Exception as e:
            logging.error("Une erreur s'est produite : %s", e)

        data_filtred = {
                            'Time': data_weather['dt'],
                            'Weather': data_weather['weather'][0]['main'],
                            'Rain Volume (mm)': data_weather.get('rain', {}).get('1h', 0),
                            'Temperature (Celsius)' : round(data_weather['main']['temp'] - 273.15, 2),
                            'Wind Speed (m/s)': data_weather['wind']['speed'],
                            'Humidity (%)': data_weather['main']['humidity'],
                            'Station Name' : data_weather['name']
            }
       

        dataframe = pd.DataFrame([data_filtred])
        try:
            with open("/opt/output/weather.csv", 'a', newline='') as csvfile:
                dataframe.to_csv(csvfile, header=csvfile.tell() == 0, index=False)
                logging.info("Données enregistrer dans le fichier csv")
        except Exception as e:
            logging.error("Une erreur s'est produite lors de l'écriture dans le fichier CSV : %s", e)

    def run(self):

        """
        Démarre la tâche de consommation des données météo MQTT.
        """
        try:
            self.client.connect(self.mqtt_host, self.mqtt_port)
            self.client.loop_forever()
            logging.info("Connexion au broker MQTT réussi")
        except ConnectionError as ce:
            logging.error("Erreur de connexion au broker MQTT : %s", ce)
        except TimeoutError as te:
            logging.error("Timeout lors de la connexion au broker MQTT : %s", te)
        except Exception as e:
            logging.error("Une erreur s'est produite : %s", e)



# Configuration de la journalisation
logging.basicConfig(level=logging.DEBUG,  
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Nom du service du broker MQTT dans Docker Compose
MQTT_BROKER_HOST = "mosquitto"
MQTT_BROKER_PORT  = 1883

# Création de l'instance de weather_consumer
weather_consumer = WeatherConsumer(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
weather_consumer.run()




