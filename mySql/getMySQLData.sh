#!/bin/bash

STARTROW=0
BLOCKSIZE=50

for i in "$@"
do
case $i in
    -s=*|--startRow=*)
    STARTROW="${i#*=}"
    shift # past argument=value
    ;;
    -b=*|--blockSize=*)
    BLOCKSIZE="${i#*=}"
    shift # past argument=value
    ;;
    --default)
    DEFAULT=YES
    shift # past argument with no value
    ;;
    *)
          # unknown option
    ;;
esac
done
echo "STARTROW = ${STARTROW}"
echo "BLOCKSIZE = ${BLOCKSIZE}"

gradle runFlightRecorderMySqlImportJob -PstartRow=${STARTROW} -PblockSize=${BLOCKSIZE}