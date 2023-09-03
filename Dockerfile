FROM python:3.9

WORKDIR /app

copy . /app/

run pip install -r requirements.txt
cmd python3 -u -m dealer --config config.json 
