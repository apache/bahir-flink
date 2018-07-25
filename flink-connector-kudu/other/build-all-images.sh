#!/usr/bin/env bash

#cd role
sudo docker build -t eskabetxe:kudu /role
#cd ..

sudo docker-compose build
sudo docker-compose up
