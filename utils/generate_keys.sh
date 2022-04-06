#! /bin/bash

if [ "$#" -ne 3 ]; then
    echo "usage: <ENV> <container_name> <PHONE>"
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
# save phone

SKIP_SMS=''

if [ "$1" = "dev" ]; then
   login_endpoint="https://login-dev.leverex.com"
   api_endpoint="wss://rollthecoin-dev.leverex.com:443"
   echo "{\"phone\":\"$3\",\"login_endpoint\" : \"$login_endpoint\",\"api_endpoint\" : \"$api_endpoint\"}" >> $config_file
   SKIP_SMS='--skip_sms'
elif [ "$1" = "live" ]; then
   login_endpoint="https://login-live.leverex.com"
   api_endpoint="wss://api-live.leverex.com:443"
   echo "{\"phone\":\"$3\",\"login_endpoint\" : \"$login_endpoint\",\"api_endpoint\" : \"$api_endpoint\"}" >> $config_file
   # NOTE: LIVE is in dev mode now, so SMS validation is disabled
   SKIP_SMS='--skip_sms'
else
   echo "{\"phone\":\"$3\"}" >> $config_file
fi

# generate private key and save
openssl ecparam -name prime256v1 -genkey -noout -out $private_key_file
if [ $? -ne 0 ]; then
   echo "ERROR: failed to create private key"
   exit 1
fi

pipenv run python add_container_key.py --keys_path $keys_dir $SKIP_SMS
if [ $? -ne 0 ]; then
   echo "ERROR: failed to add and confirm key on login service"
   exit 1
fi
exit 0
