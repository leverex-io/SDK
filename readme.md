# Getting ready

## Pre-config machine

 - install python3
 - install pipenv

## Add your key to a platfom

- naviage to utils dir in terminal
- run autheid_upload_key.sh with required paramenters. for example

```bash
./autheid_upload_key.sh dev api_key_1 my.account@mail.com
```

This will generate and register new key for account my.account@mail.com on dev environment and will store it in dev/api_key_1/key.pem file.
