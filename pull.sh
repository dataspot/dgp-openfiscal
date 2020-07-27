#!/bin/bash
git pull
source secrets.prod.env
docker pull akariv/dgp-app:latest
docker-compose up --build -d
docker system prune -a