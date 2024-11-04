from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import logging
import json


class NASADataIngestion:

    def __init__(self, api_key, api_endpoint, kafka_server, topic):
        self.logger = logging.getLogger(__name__)
        self.start_date = datetime.now().date()
        self.total_NEOs_fetched = 0
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.kafka_server = kafka_server
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_server,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def fetch_neo_data(self):
        """Fetch data from NASA NEO API."""

        self.logger.info(f"Ingestion: Fetching data from NASA NEO API. "
                         f"Start date: {self.start_date}. "
                         f"End date: {self.start_date + timedelta(days=1)}.")

        # Set params for API call using an incremental 2-day window
        params = {
            'start_date': self.start_date,
            'end_date': self.start_date + timedelta(days=1),
            'api_key': self.api_key,
        }

        # Fetch NEOs
        response = requests.get(url=self.api_endpoint, params=params)

        # Update start_date for next API call
        self.start_date = self.start_date + timedelta(days=2)

        return response.json()

    def stream_to_kafka(self, data):
        """Stream data to Kafka topic."""

        # Iterate over dates in NEO data
        for date in data['near_earth_objects']:

            neos_fetched = len(data['near_earth_objects'][date])
            self.total_NEOs_fetched += neos_fetched
            self.logger.info(f"Ingestion: Streaming NASA NEO API data to Kafka. "
                             f"Date: {date}. "
                             f"Count: {neos_fetched}. "
                             f"Cumulative count: {self.total_NEOs_fetched}.")

            # Stream each NEO (Near Earth Object) for the given date to Kafka
            for neo in data['near_earth_objects'][date]:
                self.producer.send(self.topic, neo)

        # Ensure all messages are sent
        self.producer.flush()
