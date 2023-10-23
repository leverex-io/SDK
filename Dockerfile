FROM python:3.11.5

WORKDIR /app

copy . /app/

run pip install -r requirements.txt
cmd python3 -u -m dealer --config config.json 
