
list of all categories with hierarchy
local.magento/index.php/rest/V1/categories

list of products selected by search criteria
local.magento/index.php/rest/V1/products?searchCriteria[filter_groups][0][filters][0][field]=sku&
searchCriteria[filter_groups][0][filters][0][value]=24-MB0%&searchCriteria[filter_groups][0][filters][0][condition_type]=like

list of categories selected by search criteria
local.magento/index.php/rest/V1/categories/list?searchCriteria[filter_groups][0][filters][0][field]=is_active&
searchCriteria[filter_groups][0][filters][0][value]=false&searchCriteria[filter_groups][0][filters][0][condition_type]=eq


Within Magento’s root directory run below commands:
    Set Unsecure URL
    bin/magento setup:store-config:set --base-url="http://www.magento2.com/"
    Set Secure URL
    bin/magento setup:store-config:set --base-url-secure="https://www.magento2.com/"
    Clear Cache
    bin/magento cache:flush

backup mysql
docker exec docker-magento2_db_1 /usr/bin/mysqldump --password=myrootpassword -rbackfile.sql magento
docker cp docker-magento2_db_1:backfile.sql backfile.sql
restore mysql
docker cp backfile.sql proj2_db_1:backfile.sql
docker exec -i proj2_db_1 /usr/bin/mysql -uroot -pmyrootpassword magento < backfile.sql

run container
docker run --rm -p 80 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=magento mymagento:1.0

create self-signed certificate (Power shell)
PS C:\Users\s.bovsunovskyi> New-SelfSignedCertificate -Type Custom -Subject "E=test@test.com,CN=Test Docker" -DnsName "localhost", "127.0.0.1", "192.168.99.100", "192.168.99.101" -CertStoreLocation "cert:\LocalMachine\My"

copy keys to server
$ docker cp "d:/work/my_components/connectors-se/magentocms/docker/docker_private.key" 088b6f4849b4:/etc/ssl/magento_ce
rts/
$ docker cp "d:/work/my_components/connectors-se/magentocms/docker/docker_trust_cert.cer" 088b6f4849b4:/etc/ssl/magento
_certs/

product -> custom_attributes can be changed using tMap this way (replace 'url_key' for example):
jsonb_MagentoCMSInput_2.fromJson(jsonb_MagentoCMSInput_2.toJson(row4.custom_attributes).replace("wayfarer-messenger-bag", "wayfarer-messenger-bag-new"), javax.json.JsonArray.class);