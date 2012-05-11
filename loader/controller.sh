#!/bin/bash
FILE=$1
WORKERS=$2
SETSIZE=$3
PORT=$4
#WORKERS=1
coffee cleardb.coffee ${PORT} && 
for (( i=0; i <= ${WORKERS} - 1; i++ )) 
do
  echo "starting $i node"
  coffee preprocessor.coffee ${FILE} ${WORKERS} $i ${SETSIZE} ${PORT} &
#  coffee preprocessor.coffee ${FILE} ${SETSIZE} ${PORT} &
done
