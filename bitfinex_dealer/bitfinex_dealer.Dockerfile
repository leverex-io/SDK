FROM python:3.9-slim-bullseye

WORKDIR /usr/dealer_app
COPY bitfinex_dealer/requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /usr/dealer_app/trader_core
ADD trader_core/*.py .

WORKDIR /usr/dealer_app/bitfinex_dealer
ADD bitfinex_dealer/*.py .

CMD ["python", "bitfinex_dealer.py", "--config_file", "sample_config.json"]
