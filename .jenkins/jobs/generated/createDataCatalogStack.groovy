#!/usr/bin/groovy

pipeline {
    options {
        disableConcurrentBuilds()
    }
    agent {
        kubernetes {
            label 'connectors-se_data-catalog-stack'
            yaml """
            apiVersion: v1
            kind: Pod
            spec:
              containers:
                - name: maven
                  image: jenkinsxio/builder-maven:0.0.319
                  command:
                  - cat
                  tty: true
                  volumeMounts:
                  - name: docker
                    mountPath: /var/run/docker.sock
                  - name: m2
                    mountPath: /root/.m2/repository
            
              volumes:
              - name: docker
                hostPath:
                  path: /var/run/docker.sock
              - name: m2
                hostPath:
                  path: /tmp/jenkins/tdi/m2
            """.stripMargin()
        }
    }

    stages {
        stage('Temp') {
            steps {
                container('maven') {
                    echo "Configured image: ${TALEND_DATACATALOG_DATASET__DOCKER_IMAGE}"
                }
            }
        }
    }
}