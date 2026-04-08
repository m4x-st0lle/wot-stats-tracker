from src.api.wot_api import WotData
from src.api.azure_blob import BlobCon
from src.utils.logging import logging
from src.utils.chunk_list import chunk_list
from dotenv import load_dotenv
from pathlib import Path
import polars as pl
import time
import os

load_dotenv(Path(__file__).parent.parent.parent / ".env")
logger = logging.getLogger("WOT_Pipeline")

wot_data = WotData()
blob_con = BlobCon()

def flatten_json(raw_response: dict) -> list[dict]:
    # Wandelt die tief verschachtelte API-Antwort in eine flache Liste von Dictionaries um.
    data = [
        {
        "account_id": account_id,
        "tank_id": tank.get("tank_id"),
        "damage_dealt": tank.get("all",{}).get("damage_dealt",0),
        "wins": tank.get("all",{}).get("wins",0),
        "battles": tank.get("all",{}).get("battles",0)
        }
        for account_id, tanks in raw_response.items() 
        for tank in tanks if tanks
        ]
    return data

def safe_chunk_as_parquet(chunk, chunk_count):
    # Verarbeitet eine Liste von Dictionaries zu einem Polars DataFrame und lädt diesen in ein Azure Blob Storage hoch.
    df = pl.DataFrame(chunk)
    timestamp = time.strftime("%Y-%m-%d_%H-%M")
    blob_con.safe_as_parquet(df, f"wot_data/{timestamp}/player_tank_stats_{chunk_count}.parquet")


def get_vehicle_stats():
    """
    Implementiert ein Rate Limiting (time.sleep(0.05)), um API-Sperren zu verhindern.
    Eine Liste aller account_ids wird in chunks unterteilt und gespeichert Ziel ist die reduzierung der laufzeit bei einem abbruch  
    """
    
    
    top_clans = wot_data.get_top_clans(limit=5)
    clan_id = [clan.get("clan_id",{}) for clan in top_clans]
    clan_infos = wot_data.get_clan_members(clan_id)
    account_ids = [ member.get("account_id") for clan_info in clan_infos.values() for member in clan_info.get("members")]
    # TO DO: abgleich hinzufügen welche Spieler wurden bereits im Azure Storage erfasst und können übersprungen werden

    logger.info(f"Starte Abfrage für {len(account_ids)} Spieler.")

    id_chunks = chunk_list(account_ids, 5)

    chunk_count = 0

    for index, chunk in enumerate(id_chunks):
        collect = []
        chunk_count +=1

        for account_id in chunk:
            raw_response = wot_data.get_vehicle_statistics(account_id)
            data = flatten_json(raw_response)
            collect.extend(data)

            # warte um nicht das API Limit zu überschreiten
            time.sleep(0.05)

        safe_chunk_as_parquet(collect, chunk_count)
        print(f"\r{index}/{len(id_chunks)} chunks verarbeitet",end="", flush=True)
        
    logger.info("Pipeline erfolgreich abgeschlossen.")
    

#get_vehicle_stats()


