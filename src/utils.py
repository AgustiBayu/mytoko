import os
import yaml

def get_db_config():    
    """Membaca file .env"""
    return {
        "user": os.getenv("DB_USER"),
        "pass": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "name": os.getenv("DB_NAME")
    }

def load_config():
    """Membaca file config.yaml dalam local atau docker"""
    config_path = os.getenv('CONFIG_PATH', 'config.yaml')

    with open(config_path, 'r') as file:
        return yaml.safe_load(file)