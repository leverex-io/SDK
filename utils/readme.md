# In case we need to recreate virtual env

## remove old virtual env
pipenv --python /usr/bin/python3 --rm

## remove old Pipfile*
rm Pipfile*

## create new virtual env
pipenv install --python 3.10 -r requirements.txt

# script example
./autheid_upload_key.sh env_name key_name email@somedomain
