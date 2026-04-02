rm -rf classes

groovyc obstat.groovy -d classes

cd classes 

for d in /home/yanran.hfs/code/groovy-2.4.7/lib/commons-cli-1.2.jar /home/yanran.hfs/code/groovy-2.4.7/lib/groovy-2.4.7.jar /home/yanran.hfs/code/groovy-2.4.7/lib/groovy-sql-2.4.7.jar /home/yanran.hfs/code/groovy-2.4.7/lib/mysql-connector-java-5.1.35.jar
do
jar -xf $d 
done

jar -cfe obstat.jar obstat *

mv obstat.jar ../

cd ..

cat obstat.stub obstat.jar > obstat

chmod +x obstat

osscmd put obstat oss://041331/obstat
