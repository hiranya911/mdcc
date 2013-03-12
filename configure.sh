#!/bin/sh
rm mdcc1.properties mdcc2.properties zk1.properties zk2.properties app-server.properties
echo "Reading input from $1"
counter=0

for zk in "zk1.properties" "zk2.properties"
do
    echo "tickTime=2000
clientPort=30110
initLimit=5
syncLimit=2" >> ${zk}
done

for ip in `cat $1`
do
    echo "mdcc.server.${counter}=${ip}:909${counter}" >> mdcc1.properties
    echo "server.${counter}=${ip}:1011${counter}:2011${counter}" >> zk1.properties
    echo "mdcc.server.0.${counter}=${ip}:909${counter}" >> app-server.properties
    counter2=$[${counter} + 5]
    echo "mdcc.server.${counter2}=${ip}:909${counter2}" >> mdcc2.properties
    echo "server.${counter2}=${ip}:1011${counter2}:2011${counter2}" >> zk2.properties
    echo "mdcc.server.1.${counter2}=${ip}:909${counter2}" >> app-server.properties
    counter=$[${counter} + 1]
done

for ip in `cat $1`
do
    echo "Uploading configuration files to ${ip}"
    scp zk1.properties ubuntu@${ip}:~/shard0/conf/zk.properties
    scp mdcc1.properties ubuntu@${ip}:~/shard0/conf/mdcc.properties
    scp zk2.properties ubuntu@${ip}:~/shard1/conf/zk.properties
    scp mdcc2.properties ubuntu@${ip}:~/shard1/conf/mdcc.properties
    scp app-server.properties ubuntu@${ip}:~/shard0/conf/
    scp app-server.properties ubuntu@${ip}:~/shard1/conf/
done
echo "Done"
