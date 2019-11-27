magento zip
https://magento.com/tech-resources/download
Full Release with Sample Data
rename it to "magento_2.2.5_install.zip"

magento dockerfile instruction
https://cloudkul.com/blog/magento-2-docker-installation/

keys for docker SSL tests:
cert/test_docker.*

use certificates in maven:
maven install -Djavax.net.ssl.trustStore=docker/cert/test_docker.jks -Djavax.net.ssl.trustStorePassword=changeit -f pom.xml

