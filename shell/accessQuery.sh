PRG="$0"
MYHOME=`cd $(dirname "$PRG"); pwd`
hid=$1
beginTime=$2
endTime=$3
inputOrcPath=$4 #/hadoop/accessQueryTest/orc/data/
inputTextPath=$5 # /hadoop/accessQueryTest/text/0800_done/${hid},/hadoop/accessQueryTest/text/0805_done/${hid},""/hadoop/accessQueryTest/text/0810_done/${hid}

batchid=`date +%s`
runlog=${MYHOME}/logs/job-accessETL.log

httpserver1="172.18.133.198"
httpserver2="172.18.133.199"
port=8899
ifServerNormal=1

function rand(){  
    min=$1  
    max=$(($2-$min+1))  
    num=$(date +%s%N)  
    echo $(($num%$max+$min))  
}

rnd=$(rand 1 2)
taskserver1=`eval echo \\$$"httpserver${rnd}"`
if [ $rnd -eq 1 ]; then
    taskserver2=`eval echo \\$$"httpserver2"`
else
    taskserver2=`eval echo \\$$"httpserver1"`
fi
taskserver=$taskserver1


ping=`curl -o /dev/null -m 10 -s -w %{http_code} "http://${taskserver1}:${port}/ping"`
if [ "$ping" -ne "200" ]; then
    now=`date +%Y%m%d%H%M%S`
    echo "${starttime}|${now}|${taskserver1}服务down|${result}" >> ${runlog}-error
    ifServerNormal=0
    ping=`curl -o /dev/null -m 10 -s -w %{http_code} "http://${taskserver2}:${port}/ping"`

    if [ "$ping" -eq "200" ]; then
        ifServerNormal=1
        taskserver=$taskserver2
    else
        ifServerNormal=0
        now=`date +%Y%m%d%H%M%S`
        echo "${starttime}|${now}|${taskserver2}服务down|${result}" >> ${runlog}-error
    fi
else
    taskserver=$taskserver1
fi

if [ $ifServerNormal -eq 1 ]; then
    result=`curl -s -w "|%{http_code}" -H "Content-type: application/json;charset=UTF-8" -XPOST http://${taskserver}:${port}/convert -d '{"serverLine":"accessQuery","appName":"accessQuery","batchid":"'${batchid}'","hid":"'${hid}'","beginTime":"'"${beginTime}"'","endTime":"'"${endTime}"'","inputPath":"'${inputOrcPath}'","inputTextPath":"'${inputTextPath}'"}'`

    code=`echo $result|awk -F "|" '{print $2}'`
    if [ $code -eq 200 ]; then
        now=`date +%Y%m%d%H%M%S`
        echo "${starttime}|${now}|pgwetl|${result}|${trytime}|${taskserver}" >> ${runlog}
    else [ $code -ne 200 ]
         let "trytime++"
         now=`date +%Y%m%d%H%M%S`
         echo "${starttime}|${now}|pgwetl|${result}|${trytime}|${taskserver}" >> ${runlog}-error
    fi
fi

