FROM solr:7.4.0
MAINTAINER Sergii Khylko sergii.khylko@talend.com

RUN bin/solr start -p 8983 \
&& bin/solr create_core -c testcore \
&& bin/post -c testcore example/exampledocs/manufacturers.xml \
&& bin/solr stop -p 8983

COPY solr-ssl.keystore.jks /opt/solr/server/etc 
COPY security.json /opt/solr/server/solr/

ENV SOLR_SSL_ENABLED=true \
  SOLR_SSL_KEY_STORE=etc/solr-ssl.keystore.jks \
  SOLR_SSL_KEY_STORE_PASSWORD=secret \
  SOLR_SSL_TRUST_STORE=etc/solr-ssl.keystore.jks \
  SOLR_SSL_TRUST_STORE_PASSWORD=secret \
  SOLR_SSL_NEED_CLIENT_AUTH=false \
  SOLR_SSL_WANT_CLIENT_AUTH=false \
  SOLR_SSL_CHECK_PEER_NAME=true \
  SOLR_SSL_KEY_STORE_TYPE=JKS \
  SOLR_SSL_TRUST_STORE_TYPE=JKS

HEALTHCHECK --interval=5s --retries=10 CMD curl --fail http://localhost:8983/ || exit 1
