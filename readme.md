# 1. Build

## Image for desktop

```bash
docker build -t trading_client:latest -f trading_client.dockerfile .

docker build -t trading_dealer:latest -f trading_dealer.dockerfile .
```

## Image for raspberry PI

### Build

```bash
docker buildx build --platform linux/arm/v7 -t trading_client:latest-arm32v7 -f trading_client.dockerfile .
```

### export image

```bash
docker save trading_client -o trading_client.tar.gz
```

### Upload to Raspberry Pi

through ssh

### Import image on Raspberry Pi

```bash
sudo docker load < trading_client.tar.gz
```

### 2. Intial Influx DB setup

- disable any trading clients
- start compose file
- login to influx admin page
- get admin user token from data

# 3. Run multiple instances at once

Put influx db token to every client instance you want to start

Edit compose file to set required number of entries.
Updte compose file with API endpoint and API keys.

```bash
docker-compose up
```

### 3. Setup keys

0. Pre-config

install pipenv

```bash
pipenv install -r requirements.txt
```

1.a. Generate and register keys for container with phone

from trading_client dir execute and input SMS code once asked for it
```bash
./generate_keys.sh <ENV> <container_name> <PHONE>
```
example
```bash
./generate_keys.sh dev dealing_client_dev_1 +123456789012
```

1.b. Generate and register keys for container with eId

Use email that is registered with Auth eID and have active device to sign requests.

Run
```bash
./autheid_upload_key.sh <ENV> <container_name> <EMAIL>
```
example
```bash
./autheid_upload_key.sh dev dealing_client_dev_1 test@email.com
```
### 4. Generate suite

#### Validate appropriate config file for a test suite

For example dev_config.json
Make sure that correct login service URL is used in "login_endpoint"

For REST login client ( legacy. will be deprecated)
```
https://login-dev.leverex.io
```

For WS login client
```
wss://login-dev.leverex.io/ws/v1/websocket
```

# Run admin API test script

```bash
pipenv run python test_admin_api.py --configs_path="./dev/dealing_client_dev_1" --api_endpoint="ws://api-admin-dev.leverex.local" --login_endpoint="https://login-dev.leverex.com" --api_endpoint_for_challenge="wss://api-admin-dev.leverex.local"
```
