FROM registry.datapwn.com/dchmyga/components-integration-test-mongodb:1.0.0

COPY server.pem /mongocerts/
COPY ca.pem /mongocerts/

HEALTHCHECK --interval=5s CMD echo 'db.stats().ok' | mongo --ssl --sslCAFile /mongocerts/ca.pem --host localhost