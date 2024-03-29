#! /bin/bash

if [ "$#" -ne 3 ]; then
    echo "usage: <ENV> <container_name> <EMAIL>"
    exit 1
fi

# create ENV keys dir
mkdir -p $1
if [ $? -ne 0 ]; then
   echo "ERROR: failed to create env dir: $1"
   exit 1
fi

# check that there is no keys dir for container
if [ -d $1/$2 ]; then
   echo "ERROR: keys dir for $2 already exists for $1"
   exit 1
fi

# create keys dir for container
mkdir $1/$2
if [ $? -ne 0 ]; then
   echo "ERROR: failed to create keys dir: $1/$2"
   exit 1
fi

keys_dir=$1/$2
config_file="$keys_dir/config.json"
private_key_file="$keys_dir/key.pem"
public_key_file="$keys_dir/public_key.pem"

if [ "$1" = "dev" ]; then
   login_endpoint="wss://login-dev.leverex.io/ws/v1/websocket"
   api_endpoint="wss://api-dev.leverex.io"
   service_url="wss://dev.leverex.io"
   echo "{\"email\":\"$3\",\"login_endpoint\": \"$login_endpoint\",\"api_endpoint\": \"$api_endpoint\",\"service_url\": \"$service_url\"}" >> $config_file

elif [ "$1" = "devprem" ]; then
   login_endpoint="wss://login-devprem.leverex.io/ws/v1/websocket"
   api_endpoint="wss://api-devprem.leverex.io"
   service_url="wss://api-devprem.leverex.io"
   echo "{\"email\":\"$3\",\"login_endpoint\": \"$login_endpoint\",\"api_endpoint\": \"$api_endpoint\",\"service_url\": \"$service_url\"}" >> $config_file

elif [ "$1" = "uat" ]; then
   login_endpoint="wss://login-testnet.leverex.io/ws/v1/websocket"
   api_endpoint="wss://api-testnet.leverex.io"
   service_url="wss://api-testnet.leverex.io"
   echo "{\"email\":\"$3\",\"login_endpoint\": \"$login_endpoint\",\"api_endpoint\": \"$api_endpoint\",\"service_url\": \"$service_url\"}" >> $config_file

elif [ "$1" = "live" ]; then
   login_endpoint="wss://login-live.leverex.io/ws/v1/websocket"
   api_endpoint="wss://api-live.leverex.io"
   service_url="wss://api-live.leverex.io"
   echo "{\"email\":\"$3\",\"login_endpoint\": \"$login_endpoint\",\"api_endpoint\": \"$api_endpoint\",\"service_url\": \"$service_url\"}" >> $config_file

elif [ "$1" = "prod" ]; then
   login_endpoint="wss://login.leverex.io/ws/v1/websocket"
   api_endpoint="wss://api.leverex.io"
   service_url="wss://api.leverex.io"
   echo "{\"email\":\"$3\",\"login_endpoint\": \"$login_endpoint\",\"api_endpoint\": \"$api_endpoint\",\"service_url\": \"$service_url\"}" >> $config_file

else
   echo "ERROR: undefined environment: $1"
   rm -r $1/$2
   exit 1
fi

# generate private key and save
openssl ecparam -name prime256v1 -genkey -noout -out $private_key_file
if [ $? -ne 0 ]; then
   echo "ERROR: failed to create private key"
   rm -r $1/$2
   exit 1
fi

pipenv install -r requirements.txt --python "3.9"

pipenv run python add_container_key.py --keys_path $keys_dir
if [ $? -ne 0 ]; then
   echo "ERROR: failed to add and confirm key on login service"
   rm -r $1/$2
   exit 1
fi
