import os
import time
import logging
#from collections import deque

import rsi_queue
import talib
import numpy as np
from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_client import SpotWebsocketClient as Client

LOG_DIR = os.path.join(os.getcwd(), 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'bot_log.txt')

if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

config_logging(logging, logging.DEBUG, log_file='./logs/bot_log.txt')

RSI_PERIOD = 8
#closes = deque(maxlen=RSI_PERIOD+1)
closes = rsi_queue.RedisQueue('closes', maxlen=RSI_PERIOD+1, db=1)
closes.clear()

last_rsi = None

def message_handler(message, rsi_period=RSI_PERIOD):
    global closes, last_rsi
    
    if not isinstance(message, dict):
        return

    candlestick = message.get('k')
    if candlestick is None:
        return

    symbol = candlestick.get('s', '')
    close_price = candlestick.get('c', '')
    is_end = candlestick.get('x', False)

    if symbol and close_price and is_end:
        data = {'symbol': symbol,
                'close_price': float(close_price)}
        closes.put(data)
        print('Added new close price.')

    if closes.qsize() > RSI_PERIOD:
        data = [rec['close_price']  for rec in closes.read_all()]
        rsi = talib.RSI(np.array(data), rsi_period)
        if last_rsi is None or last_rsi != rsi[-1]:
            last_rsi = rsi[-1]
            print('RSI value:', last_rsi)

if __name__ == '__main__':
    c = Client()
    c.start()
    c.kline(symbol="ethusdt", id=1, interval="1m", callback=message_handler)
    time.sleep(700)
    logging.debug("closing websocket connection")
    c.stop()
    closes.flushdb()
