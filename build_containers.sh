#! /bin/bash
docker build -t trading_client:latest -f trading_client.dockerfile .
docker build -t trading_dealer:latest -f trading_dealer.dockerfile .
