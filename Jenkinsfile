def slackChannel = 'components-ci'
def version = 'will be replaced'
def image = 'will be replaced'

def deploymentSuffix = env.BRANCH_NAME == "master" ? "" : ("tdi/${env.BRANCH_NAME}")
def deploymentRepository = "https://artifacts-zl.talend.com/nexus/content/repositories/snapshots/${deploymentSuffix}"

def createContainer(name) {
    """- name: ${name}
      image: jenkinsxio/builder-maven:0.1.60
      command:
      - cat
      tty: true
      volumeMounts:
      - name: docker
        mountPath: /var/run/docker.sock
      - name: m2${name}
        mountPath: /root/.m2/repository"""
}

pipeline {
  agent {
    kubernetes {
      label 'connectors-se'
      yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
    ${createContainer('main')}
    ${createContainer('docker')}
    ${createContainer('nexus')}

  volumes:
  - name: docker
    hostPath:
      path: /var/run/docker.sock
  - name: m2main
    hostPath:
      path: /tmp/jenkins/tdi/m2_main
  - name: m2nexus
    hostPath:
      path: /tmp/jenkins/tdi/m2_nexus
  - name: m2docker
    hostPath:
      path: /tmp/jenkins/tdi/m2_docker
"""
    }
  }

  environment {
    MAVEN_OPTS='-Dmaven.artifact.threads=128 -Dorg.slf4j.simpleLogger.showThreadName=true -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss'
    TALEND_REGISTRY='registry.datapwn.com'
  }

  parameters {
    string(
      name: 'COMPONENT_SERVER_IMAGE_VERSION',
      defaultValue: '1.1.2_20181108161652',
      description: 'The Component Server docker image tag')
  }

  options {
    buildDiscarder(logRotator(artifactNumToKeepStr: '5', numToKeepStr: env.BRANCH_NAME == 'master' ? '10' : '2'))
    timeout(time: 60, unit: 'MINUTES')
    skipStagesAfterUnstable()
  }

  triggers {
    cron(env.BRANCH_NAME == "master" ? "@daily" : "")
  }

  stages {
    stage('Run maven') {
      steps {
        container('main') {
          sh 'mvn clean install -T1C -Pdocker'
        }
      }
      post {
        always {
          junit testResults: '*/target/surefire-reports/*.xml', allowEmptyResults: true
          publishHTML (target: [
            allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true,
            reportDir: 'target/talend-component-kit', reportFiles: 'icon-report.html', reportName: "Icon Report"
          ])
        }
      }
    }
    stage('Post Build Steps') {
      parallel {
        stage('Docker') {
          steps {
            container('docker') {
              withCredentials([
                usernamePassword(
                  credentialsId: 'docker-registry-credentials',
                  passwordVariable: 'DOCKER_PASSWORD',
                  usernameVariable: 'DOCKER_LOGIN')
              ]) {
                sh 'mvn clean install -T1C -Pdocker -DskipTests'
                sh """
                     |chmod +x ./connectors-se-docker/src/main/scripts/docker/*.sh
                     |revision=`git rev-parse --abbrev-ref HEAD | tr / _`
                     |./connectors-se-docker/src/main/scripts/docker/all.sh \$revision
                     |""".stripMargin()
              }
            }
          }
        }
        stage('Site') {
          steps {
            container('main') {
              sh 'mvn clean site site:stage -T1C -Dmaven.test.failure.ignore=true'
            }
          }
          post {
            always {
              publishHTML (target: [
                allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                reportDir: 'target/staging', reportFiles: 'index.html', reportName: "Maven Site"
              ])
            }
          }
        }
        stage('Nexus') {
          steps {
            container('nexus') {
              withCredentials([
                usernamePassword(
                  credentialsId: 'nexus-artifact-zl-credentials',
                  usernameVariable: 'NEXUS_USER',
                  passwordVariable: 'NEXUS_PASSWORD')
              ]) {
                sh "mvn -s .jenkins/settings.xml clean deploy -e -DskipTests -DaltDeploymentRepository=talend.snapshots::default::${deploymentRepository}"
              }
            }
          }
        }
      }
    }
  }
  post {
    success {
      slackSend (color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
      script {
        if (env.COMPONENT_SERVER_IMAGE_VERSION) {
          println "Launching Connectors EE build with component server docker image >${env.COMPONENT_SERVER_IMAGE_VERSION}<"
          build job: '/connectors-ee/master',
                parameters: [ string(name: 'COMPONENT_SERVER_IMAGE_VERSION', value: "${env.COMPONENT_SERVER_IMAGE_VERSION}") ],
                wait: false, propagate: false
          }
      }
    }
    failure {
      slackSend (color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
    }
  }
}
