#!/bin/bash
###########################
echo start mysql
touch /var/run/mysqld/mysqld.sock
touch /var/run/mysqld/mysqld.pid
chown -R mysql:mysql /var/lib/mysql
chown -R mysql:mysql /var/run/mysqld
chown -R mysql:mysql /var/log/mysql
chmod -R 777 /var/run/mysqld/
/etc/init.d/mysql restart

## set magento's parameters from environment variables
echo $MAGENTO_BASE_URL
echo $MAGENTO_BASE_URL_SECURE
echo $MAGENTO_USE_SECURE
echo $MAGENTO_USE_SECURE_ADMIN
set +x
su www-data <<EOSU

cd /var/www/html/bin/
./magento setup:store-config:set --base-url="$MAGENTO_BASE_URL"
./magento setup:store-config:set --base-url-secure="$MAGENTO_BASE_URL_SECURE"
./magento setup:store-config:set --use-secure="$MAGENTO_USE_SECURE"
./magento setup:store-config:set --use-secure-admin="$MAGENTO_USE_SECURE_ADMIN"
./magento cache:flush

EOSU
set -x
echo 'container setup finished'

echo start apache
echo ServerName www.example.com >> /etc/apache2/apache2.conf
source /etc/apache2/envvars && exec /usr/sbin/apache2 -DFOREGROUND

###########################

