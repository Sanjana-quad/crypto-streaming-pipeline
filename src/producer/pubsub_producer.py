import json
import time
import requests
from kafka import KafkaProducer

from src.utils.config_loader import load_config
from src.utils.logger import setup_logger

logger = setup_logger()


class CryptoProducer:
    def __init__(self):
        self.config = load_config()
        self.topic = self.config["kafka"]["topic"]
        self.bootstrap_servers = self.config["kafka"]["bootstrap_servers"]

        self.interval = self.config["producer"]["interval_seconds"]
        self.max_retries = self.config["producer"]["max_retries"]

        self.api_url = (
            self.config["api"]["base_url"]
            + self.config["api"]["endpoint"]
        )

        self.params = {
            "vs_currency": self.config["api"]["vs_currency"],
            "per_page": self.config["api"]["per_page"],
        }

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def fetch_data(self):
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(self.api_url, params=self.params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning("Rate limited. Sleeping...")
                    time.sleep(60)
                    return []
                else:
                    logger.warning(f"Bad response: {response.status_code}")
            except Exception as e:
                logger.error(f"API error: {e}")

            time.sleep(2)

        return []

    def send_to_kafka(self, data):
        for coin in data:
            self.producer.send(self.topic, coin)

        self.producer.flush()
        logger.info(f"Sent {len(data)} records to Kafka")

    def run(self):
        logger.info("Starting streaming producer...")

        while True:
            data = self.fetch_data()

            if data:
                self.send_to_kafka(data)
            else:
                logger.warning("No data fetched")

            time.sleep(self.interval)


if __name__ == "__main__":
    producer = CryptoProducer()
    producer.run()
