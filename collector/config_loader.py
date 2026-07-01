import configparser
import os
import sys

def get_base_path():
    """Returns the base path of the executable or script."""
    if getattr(sys, 'frozen', False):
        # Running as a packaged .exe
        return os.path.dirname(sys.executable)
    else:
        # Running as a .py script
        return os.path.dirname(os.path.abspath(__file__))

def load_config():
    """Loads and parses the config.ini file."""
    base_path = get_base_path()
    config_path = os.path.join(base_path, 'config.ini')
    
    if not os.path.exists(config_path):
        print(f"[ERROR] Configuration file not found at: {config_path}")
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Extract parameters into a dictionary
    try:
        return {
            'bootstrap_server': config.get('kafka', 'bootstrap_server'),
            'topic': config.get('kafka', 'topic'),
            'dry_run': config.getboolean('collector', 'dry_run', fallback=False),
            'sample_rate_hz': config.getint('collector', 'sample_rate_hz'),
            'buffer_size': config.getint('collector', 'buffer_size')
        }
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"[ERROR] Invalid config.ini: {e}")
        sys.exit(1)
