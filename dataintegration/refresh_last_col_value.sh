lastVal=`hadoop fs -cat $1 | tail -1`
IFS=","; declare -a Array=($lastVal)
lowerBound=${Array[5]}
echo lowerBound=${lowerBound}
dateNow=`date +%d-%h-%Y`
targetDirYear=`date +'%Y'`
targetDirMonth=`date +'%b'`
targetDirDate=`date +'%d'`
targetDirHour=`date +'%H'`
targetDirMinute=`date +'%M'`
echo upperBound=${dateNow}
echo targetDirYear=${targetDirYear}
echo targetDirMonth=${targetDirMonth}
echo targetDirDate=${targetDirDate}
echo targetDirHour=${targetDirHour}
echo targetDirMinute=${targetDirMinute}
echo  ${targetDirYear},${targetDirMonth},${targetDirDate},${targetDirHour},${targetDirMinute},${dateNow} | hadoop fs -appendToFile - $1
