#!/bin/bash
NROFBLOCKS=1
STARTROW=0
BLOCKSIZE=100
ESYEAR=2015
ESMON=10
ESDAY=1
ENVIRONMENT=local
GRADLE_OPTS=-Xmx4G

for i in "$@"
do
case $i in
    -n=*|--nrofblock=*)
    NROFBLOCKS="${i#*=}"
    shift # past argument=value
    ;;
    -e=*|--environment=*)
    ENVIRONMENT="${i#*=}"
    shift # past argument=value
    ;;
    -s=*|--startRow=*)
    STARTROW="${i#*=}"
    shift # past argument=value
    ;;
    -b=*|--blockSize=*)
    BLOCKSIZE="${i#*=}"
    shift # past argument=value
    ;;
    -t=*|--table=*)
    ESTABLE="${i#*=}"
    shift # past argument=value
    ;;
    -y=*|--year=*)
    ESYEAR="${i#*=}"
    shift # past argument=value
    ;;
    -m=*|--month=*)
    ESMON="${i#*=}"
    shift # past argument=value
    ;;
    -d=*|--month=*)
    ESDAY="${i#*=}"
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
ESTABLE=5hz${ESYEAR}

echo "NROFBLOCKS = ${NROFBLOCKS}"
echo "BLOCKSIZE = ${BLOCKSIZE}"
echo "ENVIRONMENT = ${ENVIRONMENT}"
echo "ESTABLE = ${ESTABLE}"
echo "ESYEAR = ${ESYEAR}"
echo "ESMON = ${ESMON}"
echo "ESDAY = ${ESDAY}"
START=0

for s in $(eval echo "{$START..$NROFBLOCKS}")
do
STARTROW=$((s*BLOCKSIZE))
echo "STARTROW = ${STARTROW}"
gradle -PenvironmentName=${ENVIRONMENT} runFlightRecorderElasticSearchJob -PesTable=${ESTABLE} -PesYear=${ESYEAR} -PesMon=${ESMON} -PesDay=${ESDAY} -PstartRow=${STARTROW} -PblockSize=${BLOCKSIZE}
done

