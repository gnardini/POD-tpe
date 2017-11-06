mvn clean install
java -Daddresses=10.17.68.19 -Dquery=1 -DinPath=census100.csv -DoutPath=out.txt -DtimeOutPath=time.txt -Dn=10 -Dprov="Santa Fe" -cp target/grupo3-1.0-SNAPSHOT.jar Client
