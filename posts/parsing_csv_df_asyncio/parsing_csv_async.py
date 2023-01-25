import asyncio
import aiohttp
import logging
from asyncio import Queue
from aiohttp import ClientSession
from io import StringIO
import time
import pandas as pd
from codetiming import Timer

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

async def parser(parser_id: int, queue: Queue, session: ClientSession, **parsing_parameters):
   
    while True:
        url: str = await queue.get()
        await parse_csv(url, parser_id, queue, session, **parsing_parameters)
        queue.task_done()


async def parse_csv(url: str, parser_id: int, queue: Queue, session: ClientSession, **parsing_parameters):
    try:
        with Timer(text=f"In `parser_id`={parser_id}, `url`={url}, transformation took: {{:.4f}}"):
            logger.info(f"For `parser_id`={parser_id}, getting the `url`={url}")
            response = await asyncio.wait_for(session.get(url), timeout=5)
            csv = StringIO(await response.text())
            logger.info(f"`parser_id`={parser_id} has started to parse CSV file for `url`={url}.")
            dataframe = pd.read_csv(csv, **parsing_parameters)
            # simulate long parsing process
            time.sleep(0.5)
            logger.info(f"`parser_id`={parser_id} has finished parsing the CSV file for `url`={url}.")
            logger.info(f"Print out first five columns of the first row of the dataframe: {dict(dataframe.iloc[0, :5])}")
    except Exception as e:
        logging.exception(f'Error processing `url`={url}')


async def main():
    with Timer(text=f"With async Queue -> Entire Run took: {{:.4f}}"):
        url_queue = Queue()
        [url_queue.put_nowait(url) for url in urls]
        logger.info(f"The `url_queue` has {url_queue}")
        async with aiohttp.ClientSession() as session:
            parsers = [asyncio.create_task(parser(i, url_queue, session, encoding='utf-8'))
                    for i in range(5)]
            await url_queue.join()
            [p.cancel() for p in parsers]

if __name__ == '__main__':
    asyncio.run(main())