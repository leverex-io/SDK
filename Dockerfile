FROM python:3.12

WORKDIR /app

copy . /app/

run pip install -r requirements.txt
cmd python3 -u -m dealer --config config.json 
