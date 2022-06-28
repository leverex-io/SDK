Notes:
 bitfinex api is not supported by python 3.10. Python 3.9 should be used


# Configuration


Please use sample_config.json as starting example

## Authentication

### Leverex

- set correct email
- set path to key file to key_file_path field

### Bitfinex

- set API key and secret string to corresponding fields


## Hedging

leverex_product - product name on Leverex platform. For example: xbtusd_rf
bitfinex_futures_hedging_product - futures product on Bitfinex platform. Paper trading product is tTESTBTCF0:TESTUSDTF0
bitfinex_order_book_len - initial size of bitfinex order book size. Please refer for [official documentation](https://docs.bitfinex.com/reference/ws-public-books)
bitfinex_order_book_aggregation - Level of price aggregation (P0, P1, P2, P3, P4).

Hedging settings:
fixed_volume - single offer volume that will be sent to leverex platform
price_ratio - price change ratio in %. ask*=(1+price_ratio), bid*=(1-price_ratio)


## Running script

- navigate to bitfinex_delaer in your terminal
- install python3 and pipenv
- config virtual env ( should be done only once)
```bash
pipenv install -r requirements.txt --python "3.9"
```
- run script
```bash
pipenv run python bitfinex_dealer.py --config_file sample_config.json
```
