FROM python:3.12

WORKDIR /app

COPY . /app/

RUN python3 -m pip install bitfinex-api-py==3.0.0b1
RUN pip install -r requirements.txt
cmd python3 -u -m dealer --config config.json
