import configparser
import os

DEFAULT_CONFIG_DIR = os.path.expanduser('~/.ditag')
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_DIR, 'config.ini')

def get_config(config_file=DEFAULT_CONFIG_FILE):
    """Reads the configuration file and returns a config object."""
    config = configparser.ConfigParser()
    if os.path.exists(config_file):
        config.read(config_file)
    return config

def save_config(config, config_file=DEFAULT_CONFIG_FILE):
    """Saves the configuration object to the config file."""
    if not os.path.exists(DEFAULT_CONFIG_DIR):
        os.makedirs(DEFAULT_CONFIG_DIR)
    with open(config_file, 'w') as f:
        config.write(f)

def get_default_config():
    """Creates a default configuration object."""
    config = configparser.ConfigParser()
    config['DEFAULT'] = {
        'database': os.path.join(DEFAULT_CONFIG_DIR, 'dicom.db'),
        'archive_path': ''
    }
    config['PACS'] = {
        'destination': '',
        'port': '',
        'aetitle': ''
    }
    return config
