import logging

# 1. Grundkonfiguration (am Anfang des Skripts)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"), # Schreibt in eine Datei
        logging.StreamHandler()              # Zeigt es in der Konsole an
    ]
)
