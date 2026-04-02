#!/bin/bash

SERVER=observer.master
PROXY=obproxy.master

rm -f ./$SERVER
rm -f ./$PROXY
wget "http://11.166.86.153:8877/$SERVER"
wget "http://11.166.86.153:8877/$PROXY"

mv $SERVER ./bin/observer
mv $PROXY ./bin/obproxy

chmod 755 ./bin/observer ./bin/obproxy
