from src.utils.logging import logging
import requests
import os


logger = logging.getLogger("WOT_Pipeline")

class WotData:
    def __init__(self):
        self.api_key = os.getenv("WOT_APPLICATION_ID")
        self.base_url = "https://api.worldoftanks.eu"
        self.logger = logging.getLogger(__name__)

    def get_top_clans(self, limit):
        endpoint = f"{self.base_url}/wot/clanratings/top/"
        params = {
            "application_id": self.api_key,
            "rank_field": "efficiency",
            "limit" : limit
        }
        respones = requests.get(endpoint, params = params)
        clans_data = respones.json().get("data")
        return clans_data

    def get_clan_members(self, clan_id:list):
        endpoint = f"{self.base_url}/wot/clans/info/"
        params = {
            "application_id": self.api_key,
            "clan_id": clan_id
        }
        respones = requests.get(endpoint, params = params)
        clan_members = respones.json().get("data")
        return clan_members

    def get_players(self, search_term, limit):
        endpoint = f"{self.base_url}/wot/account/list/"
        params = {
            "application_id": self.api_key,
            "search": search_term,
            "limit":limit
        }
        respones = requests.get(endpoint, params = params)
        players_data = respones.json().get("data")
        return players_data

    def get_vehicle_statistics(self, account_id:list):
        endpoint = f"{self.base_url}/wot/tanks/stats/"
        
        params = {
            "application_id": self.api_key,
            "account_id": account_id,
            "fields": "tank_id,all.battles,all.wins,all.damage_dealt",
            "in_garage": 1
        }
        try:
            response = requests.get(endpoint, params = params)
            response.raise_for_status()
            tanks_data = response.json()

            if tanks_data.get("status") == "error":
                logger.error(f"API Error für ID {account_id}:S {tanks_data.get('error')}")
                return None
                   
            return tanks_data.get("data")
        
        except Exception as e:
            logger.error(f"Request fehlgeschlagen für ID {account_id}: {e}")
            return None
