FROM registry.datapwn.com/vizotenko/components-integration-test-rabbitmq:1.0.1

COPY rabbitmq.config /bitnami/rabbitmq/conf/
COPY ca_certificate.pem /bitnami/rabbitmq/certificates/
COPY server_certificate.pem /bitnami/rabbitmq/certificates/
COPY server_key.pem /bitnami/rabbitmq/certificates/

HEALTHCHECK --interval=5s --retries=20 CMD curl --fail http://localhost:15672/ || exit 1
#HEALTHCHECK --timeout=300s CMD curl --fail http://localhost:15672/ || exit 1