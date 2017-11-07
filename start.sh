mvn clean install -q

cd client/target/
tar -xzf  pod-client-1.0-SNAPSHOT-bin.tar.gz
cd pod-client-1.0-SNAPSHOT
chmod u+x run*
cd ../../..
