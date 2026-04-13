import os
import yaml
from dotenv import load_dotenv


load_dotenv()


def _get_value(env_name, config_data, *path, default=None, cast=str):
    value = os.getenv(env_name)
    if value is None:
        current = config_data or {}
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        value = current if current is not None else default

    if value is None:
        return None

    if cast is int:
        return int(value)

    return value

def load_config(config_path="configs/configs.yaml"):
    config_data = {}

    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as file:
            config_data = yaml.safe_load(file) or {}

    return {
        "kafka": {
            "bootstrap_servers": _get_value(
                "KAFKA_BOOTSTRAP_SERVERS",
                config_data,
                "kafka",
                "bootstrap_servers",
                default="localhost:9092",
            ),
            "topic": _get_value(
                "KAFKA_TOPIC",
                config_data,
                "kafka",
                "topic",
                default="crypto_prices",
            ),
        },
        "producer": {
            "interval_seconds": _get_value(
                "PRODUCER_INTERVAL_SECONDS",
                config_data,
                "producer",
                "interval_seconds",
                default=30,
                cast=int,
            ),
            "max_retries": _get_value(
                "PRODUCER_MAX_RETRIES",
                config_data,
                "producer",
                "max_retries",
                default=3,
                cast=int,
            ),
        },
        "api": {
            "base_url": _get_value(
                "API_BASE_URL",
                config_data,
                "api",
                "base_url",
                default="https://api.coingecko.com/api/v3",
            ),
            "endpoint": _get_value(
                "API_ENDPOINT",
                config_data,
                "api",
                "endpoint",
                default="/coins/markets",
            ),
            "vs_currency": _get_value(
                "API_VS_CURRENCY",
                config_data,
                "api",
                "vs_currency",
                default="usd",
            ),
            "per_page": _get_value(
                "API_PER_PAGE",
                config_data,
                "api",
                "per_page",
                default=10,
                cast=int,
            ),
        },
        "paths": {
            "stream_output": _get_value(
                "STREAM_OUTPUT_PATH",
                config_data,
                "paths",
                "stream_output",
                default="data/windowed_output",
            ),
            "checkpoint": _get_value(
                "CHECKPOINT_PATH",
                config_data,
                "paths",
                "checkpoint",
                default="data/checkpoints",
            ),
        },
    }
