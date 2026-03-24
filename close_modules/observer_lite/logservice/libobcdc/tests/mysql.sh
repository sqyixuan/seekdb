#!/bin/bash

if [ $# -lt 1 ]
then
  echo "USAGE: ./mysql.sh ${IP} ${PORT} to connect sys tenant with default passoword and default database"
  echo "    OR ./mysql.sh ${IP} {PORT} {USER} {PASSWD} to connect ob with specified user/passoword and default database"
  echo "    OR ./mysql.sh ${IP} {PORT} {USER} {PASSWD} {DB} to connect ob with specified user/passoword and specified database"
elif [ $# -lt 2 ]
then
  echo "not enough args, pleasy execute ./mysql.sh to check help info"
else
  IP=$1
  PORT=$2
  DB=oceanbase
  if [ $# -eq 2 ]
  then
    USER=root
    echo "connect to sys tenant of ob{IP:$IP PORT:$PORT}"
    mysql -A -h$IP -P$PORT -u$USER -D$DB
  else
    USER=$3
    if [ $# -gt 3 ]
    then
      PASSWD=$4
    fi
    if [ $# -eq 5 ]
    then
      DB=$5
    fi
    echo "connect to IP:$IP PORT:$PORT USER:$USER PASSWD:$PASSWD DB:$DB"
    mysql -A -h$IP -P$PORT -u$USER -p$PASSWD -D$DB
  fi
fi
