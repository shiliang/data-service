echo "start data service"

nohup /usr/local/bin/dataserver &

echo "finish"

while true; do
  time=$(date "+%Y-%m-%d %H:%M:%S")
  echo $time && echo "do while for docker daemon"
  sleep 300

  if ps -ef | grep -v grep | grep "/usr/local/bin/dataserver" > /dev/null
  then
      echo "data service is running."
  else
      break
  fi

done