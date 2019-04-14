#!/usr/bin/env bash
# creates simple self signed certificates to use with TCP TLS module
# by default, it will create "certs" folder in the current folder

if [ "$#" -eq 0 ] || [ -z "$1" ]; then
   echo "usage: create_certs.sh {num of replicas} {optional - output folder}"
   exit 1
fi

dir=$2
if [ -z $dir ]; then
   dir="certs"
fi

i=0
while [ $i -lt $1 ]; do
   echo "processing replica $i"
   clientDir=$dir/$i/client
   serverDir=$dir/$i/server

   mkdir -p $clientDir
   mkdir -p $serverDir

   openssl ecparam -name secp384r1 -genkey -noout -out \
$serverDir/pk.pem
   openssl ecparam -name secp384r1 -genkey -noout -out \
$clientDir/pk.pem

   openssl req -new -key $serverDir/pk.pem -nodes -days 365 -x509 \
-subj "/C=NA/ST=NA/L=NA/O=NA/OU="$i"/CN=node"$i"ser" -out $serverDir/server.cert

   openssl req -new -key $clientDir/pk.pem -nodes -days 365 -x509 \
-subj "/C=NA/ST=NA/L=NA/O=NA/OU="$i"/CN=node"$i"cli" -out $clientDir/client.cert

   let i=i+1
done

exit 0
