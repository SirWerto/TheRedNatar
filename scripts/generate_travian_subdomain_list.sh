#!/usr/bin/bash 

OUTPUTPATH=$1

echo $OUTPUTPATH
echo "subdomain" > $OUTPUTPATH
subfinder -domain travian.com -json -silent | grep --perl-regexp '\.x\d\.' | jq -r '.host' >> $OUTPUTPATH
