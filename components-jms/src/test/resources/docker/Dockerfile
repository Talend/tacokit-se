FROM webcenter/activemq:latest
COPY activemq.xml /opt/activemq/conf/

HEALTHCHECK --interval=5s CMD curl --fail http://localhost:61617/ || exit 1