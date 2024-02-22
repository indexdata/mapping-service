#!/bin/bash

if [ ! $1 ] 
then
    echo "Usage $0 <raw_marc_file>"
    exit
fi

curl 'http://localhost:8888/marc2inst' -H 'content-type: application/marc' -d "@"$1