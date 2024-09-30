echo "start data service"

# 启动数据服务并使用 Delve 进行调试
#dlv exec /home/workspace/bin/dataserver --headless --listen=:32456 --api-version=2 --accept-multiclient --log &
tail -f /home/workspace/bin/start.sh
echo "dlv finish"

