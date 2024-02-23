#!/bin/bash

if [ ! $1 ] 
then
    echo "Usage $0 <raw_marc_file> [ <starthrid> ]"
    exit
fi

if [ $2 ]
then
    START="?hridstart=${2}&hridpre=x"
fi

curl -v "http://localhost:8888/marc2inst${START}" -H 'content-type: application/marc' -d "@"$1