#!/bin/bash
if [ $# != 1 ]; then
  echo "you need give a branch"
  exit
fi
rm $1 
rm bin/observer
wget http://11.166.86.153:8877/$1
mv $1 bin/observer
chmod +x bin/observer
