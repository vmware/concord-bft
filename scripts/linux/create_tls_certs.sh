#!/usr/bin/env bash
# Creates simple self-signed certificates to use with TCP/TLS module
# by default, the script:
# 1) Creates "certs" folder in the current folder if use_unified_certificates is false
#    With unified_certs new folder tls_certs will be created 
# 2) Starts from node ID 0
#
# Examples usage:
# 1) To create 10 certificates folders with node IDs 0 to 9 in "./certs:
# > ./create_tls_certs.sh 10
#
# 2) To create 15 certificates folders with node IDs 0 to 14 in "/tmp/abc/:
# > ./create_tls_certs.sh 15 /tmp/abc
#
# 3) To create 30 certificates folders with node IDs 5 to 34 in "/tmp/fldkdsZ/:
# > ./create_tls_certs.sh 30 /tmp/fldkdsZ 5

KEY="15ec11a047f630ca00f65c25f0b3bfd89a7054a5b9e2e3cdb6a772a58251b4c2"
IV="38106509f6528ff859c366747aa04f21"

if [ "$#" -eq 0 ] || [ -z "$1" ]; then
   echo "usage: create_tls_certs.sh {num of replicas} {optional - output folder} {optional - start node ID}"
   exit 1
fi

start_node_id=$3
if [ -z "$start_node_id" ]; then
   start_node_id=0
fi

i=$start_node_id
last_node_id=$((i + $1 - 1))

use_unified_certificates=false

if [ "$use_unified_certificates" = true ]; then
   echo "Use Unified Certificates"

   dir=$2
   if [ -z "$dir" ]; then
      dir="tls_certs"
   fi

   start_c=1

   while [ $i -le $last_node_id ]; do
      echo "processing replica $i/$last_node_id"
      certDir=$dir/$i

      mkdir -p $certDir

      if [ $i -le 6 ]; then      # For replicas/TRS from 0-6, total 7
         
         k=$((i+1)) 
         openssl ecparam -name secp384r1 -genkey -noout -out $certDir/pk.pem

         openssl req -new -key $certDir/pk.pem -nodes -days 365 -x509 \
            -subj "/C=NA/ST=NA/L=NA/O=host_uuid${i}/OU=${i}/CN=concord${k}" -out $certDir/node.cert

         openssl enc -base64 -aes-256-cbc -e -in $certDir/pk.pem -K ${KEY} -iv ${IV}  \
               -p -out $certDir/pk.pem.enc 2>/dev/null

      elif [[ $i == 39 || $i == 40 ]]; then   
      # These are two principal ids of clientservice1 (39) and clientservice2 (40)
         openssl ecparam -name secp384r1 -genkey -noout -out $certDir/pk.pem

         openssl req -new -key $certDir/pk.pem -nodes -days 365 -x509 \
            -subj "/C=NA/ST=NA/L=NA/O=clientservice${start_c}/OU=${i}/CN=node${i}" -out $certDir/node.cert

         openssl enc -base64 -aes-256-cbc -e -in $certDir/pk.pem -K ${KEY} -iv ${IV}  \
               -p -out $certDir/pk.pem.enc 2>/dev/null

         (( start_c=start_c+1 ))
      else 
         openssl ecparam -name secp384r1 -genkey -noout -out $certDir/pk.pem

         openssl req -new -key $certDir/pk.pem -nodes -days 365 -x509 \
            -subj "/C=NA/ST=NA/L=NA/O=host_uuid${i}/OU=${i}/CN=node${i}" -out $certDir/node.cert

         openssl enc -base64 -aes-256-cbc -e -in $certDir/pk.pem -K ${KEY} -iv ${IV}  \
               -p -out $certDir/pk.pem.enc 2>/dev/null

      fi
      (( i=i+1 ))
   done
   # Create certs for trutil
    echo "processing client for trutil"
    clientDir=$dir/"trutil"
    mkdir -p $clientDir

    openssl ecparam -name secp384r1 -genkey -noout -out $clientDir/pk.pem

    openssl req -new -key $clientDir/pk.pem -nodes -days 36500 -x509 \
        -subj "/C=NA/ST=NA/L=NA/O=trutil/OU=trutil/CN=trutil" -out $clientDir/node.cert

    openssl enc -base64 -aes-256-cbc -e -in  $clientDir/pk.pem -K ${KEY} -iv ${IV}  \
        -p -out $clientDir/pk.pem.enc 2>/dev/null
else 
   dir=$2
   if [ -z "$dir" ]; then
      dir="certs"
   fi

   while [ $i -le $last_node_id ]; do
      echo "processing replica $i/$last_node_id"
      clientDir=$dir/$i/client
      serverDir=$dir/$i/server

      mkdir -p $clientDir
      mkdir -p $serverDir

      openssl ecparam -name secp384r1 -genkey -noout -out $serverDir/pk.pem
      openssl ecparam -name secp384r1 -genkey -noout -out $clientDir/pk.pem

      openssl req -new -key $serverDir/pk.pem -nodes -days 365 -x509 \
         -subj "/C=NA/ST=NA/L=NA/O=NA/OU=${i}/CN=node${i}ser" -out $serverDir/server.cert

      openssl req -new -key $clientDir/pk.pem -nodes -days 365 -x509 \
         -subj "/C=NA/ST=NA/L=NA/O=NA/OU=${i}/CN=node${i}cli" -out $clientDir/client.cert

      openssl enc -base64 -aes-256-cbc -e -in  $serverDir/pk.pem -K ${KEY} -iv ${IV}  \
            -p -out $serverDir/pk.pem.enc 2>/dev/null
      openssl enc -base64 -aes-256-cbc -e -in $clientDir/pk.pem -K ${KEY} -iv ${IV}  \
            -p -out $clientDir/pk.pem.enc 2>/dev/null

      # rm $serverDir/pk.pem
      # rm $clientDir/pk.pem

      (( i=i+1 ))
   done
fi
exit 0