#!/bin/bash

set -u
sleep 4
database_connectivity_check=no
var=1
while [ "$database_connectivity_check" != "mysql" ]; do
/etc/init.d/mysql start
sleep 2
database_connectivity_check=`mysqlshow --user=root --password=$MYSQL_ROOT_PASSWORD | grep -o mysql`
echo $database_connectivity_check
if [ $var -ge 4 ]; then
exit 1
fi
var=$((var+1))
done
 
 
database_availability_check=`mysqlshow --user=root --password=$MYSQL_ROOT_PASSWORD | grep -ow "$MYSQL_DATABASE"`
 
if [ "$database_availability_check" == "$MYSQL_DATABASE" ]; then
exit 1
else
mysql -u root -p$MYSQL_ROOT_PASSWORD -e "grant all on *.* to 'root'@'%' identified by '$MYSQL_ROOT_PASSWORD';"
mysql -u root -p$MYSQL_ROOT_PASSWORD -e "create database $MYSQL_DATABASE;"
mysql -u root -p$MYSQL_ROOT_PASSWORD -e "grant all on $MYSQL_DATABASE.* to 'root'@'%' identified by '$MYSQL_ROOT_PASSWORD';"

mysql -u root -p$MYSQL_ROOT_PASSWORD -e "create user $MYSQL_USER IDENTIFIED BY '$MYSQL_PASSWORD';"
mysql -u root -p$MYSQL_ROOT_PASSWORD -e "GRANT ALL ON $MYSQL_DATABASE.* TO $MYSQL_USER@localhost IDENTIFIED BY '$MYSQL_PASSWORD';"
mysql -u root -p$MYSQL_ROOT_PASSWORD -e "flush privileges;"

supervisorctl stop database_creation && supervisorctl remove database_creation
echo "Database $MYSQL_DATABASE created"
fi
 