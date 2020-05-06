 This dockerfile was prepared to test image with TLS enabled
 
 How to prepare a test image:
 
 1. Run rabbitmq container.
 2. Copy <container id>:/bitnami/ to src/main/docker/demo/bitnami
 3. Copy conf/rabbitmq.config to src/main/docker/demo/bitnami/rabbitmq/conf/
 4. Copy certificates to src/main/docker/demo/bitnami/rabbitmq
 5. Build and push rabbitmq test image with TLS support to registry:
 docker build -t vizotenko/components-integration-test-rabbitmq:1.0.0 .
 docker tag <image id> <registry>/vizotenko/components-integration-test-rabbitmq:1.0.0
 docker push <registry>/vizotenko/components-integration-test-rabbitmq:1.0.0
 