import logging
from io import StringIO
import time
import pandas as pd
from codetiming import Timer
import requests

logger =  logging.getLogger(__name__)

logging.basicConfig(
     level=logging.INFO, 
     format= '[%(asctime)s] [%(filename)s]:[%(lineno)d] [%(levelname)s] - %(message)s',
     datefmt='%H:%M:%S'
 )

urls = [
    # Some random CSV urls from the internet
    'https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/biostats.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/crash_catalonia.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/deniro.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/example.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/ford_escort.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/faithful.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/freshman_kgs.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/freshman_lbs.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/grades.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/homes.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/hooke.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/hurricanes.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/hw_25000.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/lead_shot.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/letter_frequency.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/mlb_players.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/mlb_teams_2012.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/news_decline.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/nile.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/oscar_age_female.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/oscar_age_male.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_10.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_100.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_1000.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_10000.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/tally_cab.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/trees.csv',
    'https://people.sc.fsu.edu/~jburkardt/data/csv/zillow.csv',
]

def parse_csv(url: str, **parsing_parameters):
    try:
        with Timer(text=f"{url} -> For this url, transformation took: {{:.4f}}"):
            response = requests.get(url)
            csv = StringIO(response.text)
            logger.info(f"Starting to parse CSV file for url={url}.")
            dataframe = pd.read_csv(csv, **parsing_parameters)
            # simulate long parsing process
            time.sleep(0.5)
            logger.info(f"CSV file is parsed for url={url}.")
            logger.info(f"Print out first five columns of the first row of the dataframe: {dict(dataframe.iloc[0, :5])}")
    except Exception as e:
        logging.exception(f'Error processing url {url}')


def main():
    with Timer(text=f"Normal sequential run -> Entire Run took: {{:.4f}}"):
        for url in urls:
           parse_csv(url, encoding='utf-8') 
        

if __name__ == '__main__':
    main()