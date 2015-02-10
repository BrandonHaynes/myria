cd ~/Documents/Databases/myria/
./gradlew jar
cd ~/Documents/Databases/myria/myriadeploy 
./kill_all_java_processes.py  deployment.cfg.local 
./setup_cluster.py deployment.cfg.local
./launch_cluster.sh deployment.cfg.local
sleep 3
cd ~/Documents/Databases/myria-python/
myria_upload ~/Documents/Databases/stupidDataWithDims.txt --relation smallEx --no-ssl --hostname localhost -p 8753


