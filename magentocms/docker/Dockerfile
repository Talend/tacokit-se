############################################
# layer with magento files
FROM alpine:3.5 as magento_dir
RUN apk add -U unzip && rm -rf /var/cache/apk/*

COPY magento_2.2.5_install.zip /var/www/html/
RUN cd /var/www/html && unzip magento_2.2.5_install.zip && rm magento_2.2.5_install.zip
RUN cd /var/www/html \
    && find . -type d -exec chmod 770 {} \; \
    && find . -type f -exec chmod 660 {} \; \
    && chmod 755 bin/magento

############################################

FROM ubuntu:16.04
LABEL com.talend.name="Magento CMS image for integration tests" \
      com.talend.application="tacokit" \
      com.talend.service="magentocms" \
      com.talend.version="1.0.0" \
      com.talend.description="Magento CMS image for integration tests of Magento Input and Output components" \
      com.talend.maintainer="Stanislav Bovsunovskyi <sbovsunovskyi@talend.com>" \
      com.talend.docker.cmd="docker run -d -p 55550:80 -p 55551:443 -e MAGENTO_BASE_URL=http://192.168.99.100:55550 \
        -e MAGENTO_BASE_URL_SECURE=https://192.168.99.100:55551 -e MAGENTO_USE_SECURE=0 -e MAGENTO_USE_SECURE_ADMIN=0 repository/app:tag" \
      com.talend.docker.params="MAGENTO_BASE_URL=environment variable that sets non secure Magento URL, \
        MAGENTO_BASE_URL_SECURE=environment variable that sets secure Magento URL, \
        MAGENTO_USE_SECURE=0 or 1 for non secure and secure connections respectively, \
        MAGENTO_USE_SECURE_ADMIN=0 or 1 for non secure and secure connections to admin console respectively"

## Mysql root password and PHP version to be passed as arguments while building image
ARG mysql_password=root
ARG phpV=7.1
 
## Update Server and Install LAMP
RUN apt-get update \
    && apt-get -y install apache2 \
    && a2enmod rewrite \
    && a2enmod headers \
    && export LANG=en_US.UTF-8 \
    && apt-get update \
    && apt-get install -y software-properties-common \
    && apt-get install -y language-pack-en-base \
    && apt-get install -y vim \
    && apt-get install -y unzip \
    && apt-get install -y sudo \
    && apt-get install -y curl \
    && LC_ALL=en_US.UTF-8 add-apt-repository ppa:ondrej/php \
    && apt-get update \
    && apt-get -y install php${phpV} php${phpV}-curl php${phpV}-intl php${phpV}-gd php${phpV}-dom \
    php${phpV}-mcrypt php${phpV}-iconv php${phpV}-xsl php${phpV}-mbstring php${phpV}-ctype php${phpV}-zip \
    php${phpV}-pdo php${phpV}-xml php${phpV}-bz2 php${phpV}-calendar php${phpV}-exif php${phpV}-fileinfo php${phpV}-json \
    php${phpV}-mysqli php${phpV}-mysql php${phpV}-posix php${phpV}-tokenizer php${phpV}-xmlwriter php${phpV}-xmlreader \
    php${phpV}-phar php${phpV}-soap php${phpV}-mysql php${phpV}-fpm php${phpV}-bcmath libapache2-mod-php${phpV} \
    && sed -i -e"s/^memory_limit\s*=\s*128M/memory_limit = 512M/" /etc/php/${phpV}/apache2/php.ini \
    && rm /var/www/html/* \
    && sed -i "s/None/all/g" /etc/apache2/apache2.conf \
    && apt-get -y install debconf-utils \
    && echo "mysql-server-5.7 mysql-server/root_password password ${mysql_password}" | debconf-set-selections \
    && echo "mysql-server-5.7 mysql-server/root_password_again password ${mysql_password}" | debconf-set-selections \
    && DEBIAN_FRONTEND=noninteractive apt-get -y install mysql-server-5.7 && \
    mkdir -p /var/lib/mysql && \
    mkdir -p /var/run/mysqld && \
    mkdir -p /var/log/mysql && \
    touch /var/run/mysqld/mysqld.sock && \
    touch /var/run/mysqld/mysqld.pid && \
    chown -R mysql:mysql /var/lib/mysql && \
    chown -R mysql:mysql /var/run/mysqld && \
    chown -R mysql:mysql /var/log/mysql &&\
    chmod -R 777 /var/run/mysqld/ \
    && sed -i -e"s/^bind-address\s*=\s*127.0.0.1/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf \
    && apt-get install -y supervisor \
    && mkdir -p /var/log/supervisor

ENV APACHE_RUN_USER=www-data \
 APACHE_RUN_GROUP=www-data \
 APACHE_PID_FILE=/var/run/apache2.pid \
 APACHE_RUN_DIR=/var/run/apache2 \
 APACHE_LOCK_DIR=/var/lock/apache2 \
 APACHE_LOG_DIR=/var/log/apache2 \
 LANG=C

## Copy one time database creation script from host to docker.
COPY mysql.sh /etc/mysql.sh
RUN chmod a+x /etc/mysql.sh

RUN chsh -s /bin/bash www-data

ENV MYSQL_HOST=localhost \
 MYSQL_ROOT_PASSWORD=root \
 MYSQL_USER=magento \
 MYSQL_PASSWORD=magento1 \
 MYSQL_DATABASE=magento \
 \
 MAGENTO_LANGUAGE=en_GB \
 MAGENTO_TIMEZONE=Pacific/Auckland \
 MAGENTO_DEFAULT_CURRENCY=NZD \
 MAGENTO_BACKEND_FRONTNAME=admin \
## this variables will be replaced in running container acording to current IP-address and security usage
 MAGENTO_BASE_URL=http://192.168.99.100 \
 MAGENTO_BASE_URL_SECURE=0 \
 MAGENTO_USE_SECURE=0 \
 MAGENTO_USE_SECURE_ADMIN=0 \
 \
 MAGENTO_ADMIN_FIRSTNAME=Admin \
 MAGENTO_ADMIN_LASTNAME=MyStore \
 MAGENTO_ADMIN_EMAIL=amdin@example.com \
 MAGENTO_ADMIN_USERNAME=admin \
 MAGENTO_ADMIN_PASSWORD=admin12 

## copy magento files
COPY --chown=www-data:www-data --from=magento_dir /var/www/html/ /var/www/html/

## add integration
COPY add_integr.sh /var/www/html/
RUN chmod ugo+x /var/www/html/add_integr.sh

## prepare install scripts
COPY install-magento install-sampledata docker-container-run-after.sh /usr/local/bin/
RUN chmod ugo+x /usr/local/bin/*

## run db and install magento
RUN /etc/mysql.sh && /usr/local/bin/install-magento && /usr/local/bin/install-sampledata && su www-data -c "/var/www/html/add_integr.sh"
## enable apache ssl
RUN a2enmod ssl && a2ensite default-ssl.conf

## add new certificates (instead of old)
COPY cert/test_docker.cer /etc/ssl/certs/ssl-cert-snakeoil.pem
COPY cert/test_docker.key /etc/ssl/private/ssl-cert-snakeoil.key

## Expose ports for web server and database server.
WORKDIR /var/www/html
EXPOSE 80 443
ENTRYPOINT ["/usr/local/bin/docker-container-run-after.sh"] 

HEALTHCHECK CMD curl --fail http://localhost:80/ || exit 1