import pandas as pd
import time
import logging
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
from requests.exceptions import ReadTimeout, RequestException

logging.basicConfig(level=logging.INFO)

def get_id():
    lebron = [player for player in players.get_players() if player['full_name'] == 'LeBron James'][0]
    return lebron['id']

def get_gamelog(season, player_id, retries=3, delay=5):
    for attempt in range(retries):
        try:
            logging.info(f"Fetching season {season}, attempt {attempt + 1}")
            game_log = playergamelog.PlayerGameLog(player_id=player_id, season=season)
            return game_log.get_data_frames()[0]
        except ReadTimeout:
            logging.warning(f"Timeout on season {season}, retrying in {delay} seconds...")
            time.sleep(delay)
        except RequestException as e:
            logging.error(f"Request error for season {season}: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error for season {season}: {e}")
            break
    return None

def save_gamelog():
    player_id = get_id()
    failed_seasons = []
    seasons = [f"{year}-{str(year + 1)[2:]}" for year in range(2003, 2025)]
    
    for season in seasons:
        game_log_df = get_gamelog(season, player_id)
        if game_log_df is not None:
            file_name = f"/opt/airflow/nba_data/gamelogs/{season}_Season.csv"
            game_log_df.to_csv(file_name, index=False)
            logging.info(f"Saved: {file_name}")
        else:
            failed_seasons.append(season)
        time.sleep(1.5)

    if failed_seasons:
        raise Exception(f"Failed to fetch data for seasons: {failed_seasons}")



