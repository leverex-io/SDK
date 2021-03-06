FROM python:3.10-slim-bullseye

RUN apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests -y python3-pip wget wait-for-it \
    && pip3 install influxdb-client websockets \
    && pip3 install black==21.10b0 \
    && pip3 install certifi==2021.10.8 \
    && pip3 install cffi==1.15.0 \
    && pip3 install charset-normalizer==2.0.7 \
    && pip3 install click==8.0.3 \
    && pip3 install cryptography==35.0.0 \
    && pip3 install Deprecated==1.2.13 \
    && pip3 install idna==3.3 \
    && pip3 install jwcrypto==1.0 \
    && pip3 install mypy-extensions==0.4.3 \
    && pip3 install pathspec==0.9.0 \
    && pip3 install platformdirs==2.4.0 \
    && pip3 install pycparser==2.20 \
    && pip3 install regex==2021.11.2 \
    && pip3 install requests==2.26.0 \
    && pip3 install tomli==1.2.2 \
    && pip3 install typing-extensions==3.10.0.2 \
    && pip3 install urllib3==1.26.7 \
    && pip3 install websocket-client==1.2.1 \
    && pip3 install wrapt==1.13.3 \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /usr/app

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

ADD *.py ./

CMD [ "wait-for-it", "influxdb:8086", "--strict", "--timeout=10", "--", "python", "./trading_client.py" ]
