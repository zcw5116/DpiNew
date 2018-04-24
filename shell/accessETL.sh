PRG="$0"
MYHOME=`cd $(dirname "$PRG"); pwd`


starttime=$1
if [ -z $starttime ]; then
    starttime=`date -d "1 minutes ago" "+%Y%m%d%H%M"`
fi

group=$2
if [ -z $group ]; then
    group=""
fi

BATCH_ID=${starttime}${group}
loadTime=${starttime}

runlog=${MYHOME}/logs/job-accessETL.log

httpserver1="172.18.133.199"
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

trytime=0
while(($trytime < 5)); do
  if [ $trytime -gt 0 ]; then sleep 90; fi

  ping=`curl -o /dev/null -m 10 -s -w %{http_code} "http://${taskserver1}:${port}/ping"`
  if [ "$ping" -ne "200" ]; then
      now=`date +%Y%m%d%H%M%S`
      echo "${starttime}|${now}|${taskserver1}服务down|${result}|${trytime}" >> ${runlog}-error
      ifServerNormal=0
      ping=`curl -o /dev/null -m 10 -s -w %{http_code} "http://${taskserver2}:${port}/ping"`

      if [ "$ping" -eq "200" ]; then
          ifServerNormal=1
          taskserver=$taskserver2
      else
          ifServerNormal=0
          let "trytime++"
          now=`date +%Y%m%d%H%M%S`
          echo "${starttime}|${now}|${taskserver2}服务down|${result}|${trytime}" >> ${runlog}-error
      fi
  else
      taskserver=$taskserver1
  fi

  if [ $ifServerNormal -eq 1 ]; then
      result=`curl -s -w "|%{http_code}" -H "Content-type: application/json;charset=UTF-8" -XPOST http://${taskserver}:${port}/convert -d '{"serverLine":"accessM5ETL","appName":"accessETL_'${BATCH_ID}'","inputPath":"hdfs://cdh-nn-001:8020/hadoop/accesslog/","outputPath":"hdfs://cdh-nn-001:8020/hadoop/accesslog_etl_small/output/","coalesceSize":"100","loadTime":"'${loadTime}'", "tryTime":"'${trytime}'","accessTable":"dpi_log_access_m5","ifRefreshPartiton":"0"}'`

      code=`echo $result|awk -F "|" '{print $2}'`
      if [ $code -eq 200 ]; then
          now=`date +%Y%m%d%H%M%S`
          echo "${starttime}|${now}|pgwetl|${result}|${trytime}|${taskserver}" >> ${runlog}
          break
      else [ $code -ne 200 ]
           let "trytime++"
           now=`date +%Y%m%d%H%M%S`
           echo "${starttime}|${now}|pgwetl|${result}|${trytime}|${taskserver}" >> ${runlog}-error

      fi
  fi
done

