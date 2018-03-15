the getESData shell script can be used to get data for a given day from the elastic search instance.

It needs the following input parameters:
    -n or --nrofblock
    -e or --environment
    -s or --startRow
    -b or --blockSize
    -t or --table
    -y or --year
    -m or --month
    -d or --day


So if you know how many entries there are for a given day you can pass in the nr of blocks (default is 25000, any higher usually causes java heap space issues) the year, month and day and run the script;
It will pull in data in blocks of 25000 until the last block is read in.

    ./getESData.sh -y=2015 -m=12 -d=12 -b=25000 -n=8
    
This will pull in 9 blocks of data each 25000 documents each. (on mymachine this took about 12 minutes, 290 docs per second)

Afterwards you can run the flightdata harmonization in the usecase3 folder
