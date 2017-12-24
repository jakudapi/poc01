'''
This file contains a bunch of helper classes, methods that retrieve historical data from GDAX
'''

import datetime as dt
import json
import logging
import pickle
# from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
import requests
from time import sleep
from typing import Dict, Tuple, List, NewType

HISTORICAL_URL = 'https://api.gdax.com/products/{prod_id}/candles/?start={start}&end={end}&granularity={granularity}'

PROD_URL = 'https://api.gdax.com/products/'


def get_dataslice(start: int, end: int, gran: int, prod: str) -> Dict:
  print(HISTORICAL_URL.format(prod_id=prod, start=start, end=end, granularity=gran))
  r = requests.get(HISTORICAL_URL.format(prod_id=prod, start=start, end=end, granularity=gran))
  if r.status_code != 200:
    print("Error trying to getting historical data {}".format(r.status_code))
    logging.error("Error trying to get historical data: returned {}".format(r.status_code))
  else:
    temp = {}
    for item in r.json():
      temp[item[0]] = item[1:]
    return temp


def save(data, name="dump.pkl"):
  '''
  data Dict(int:[float, float, float, float, float])
  '''
  with open(name, 'wb') as fp:
    pickle.dump(data, fp)


def load(name="dump.pkl"):
  '''
  name:str is the filename to load the serialized data
  '''
  with open(name, 'rb') as fp:
    object = pickle.load(fp)
  return object


def get_history(type="BTC-USD", start=1483228800, filename="dump.pkl"):
  '''
  type:str type of currency
  start:int , seconds since epoch. defaults to Jan 1st, 2017 UTC
  filename:str, filename to save to
  '''
  data = {}
  today = dt.datetime.now(tz=dt.timezone.utc)
  the_date = dt.datetime.fromtimestamp(start, tz=dt.timezone.utc)
  while (today - the_date).days > 0:
    start = the_date.isoformat(timespec='minutes')[:-6]
    end = (the_date + dt.timedelta(days=4)).isoformat(timespec='minutes')[:-6]
    temp = get_dataslice(start=start, end=end, gran=3600, prod="LTC-USD")
    sleep(.4)
    data.update(temp)
    the_date = the_date + dt.timedelta(days=4)
  pprint(data)
  print(len(data))
  save(data, filename)


def load_history(filename="dump.pkl"):
  with open(filename, 'rb') as fp:
    return pickle.load(fp)


class Gdax(object):
  def __init__(self):
    self._products = {}

  def refresh(self):
    '''
    refresh the available products from gdax and...  *what else*?
    '''
    r = requests.get(PROD_URL)
    if r.status_code != 200:
      print("Error trying to get products: returned {}".format(r.status_code))
      logging.error("Error trying to get products: returned {}".format(r.status_code))
    else:
      self._products = {}
      for prod in r.json():
        self._products[prod['id']] = prod


if __name__ == "__main__":
  session = Gdax()
  session.refresh()
  pprint(session._products)
  #get_history(type="LTC-USD", filename="ltc_2017.pkl")
  pprint(load_history("ltc_2017.pkl"))



