def slackChannel = 'components-ci'

def deploymentSuffix = env.BRANCH_NAME == "master" ? "" : ("tdi/${env.BRANCH_NAME}")
def deploymentRepository = "https://artifacts-zl.talend.com/nexus/content/repositories/snapshots/${deploymentSuffix}"

pipeline {
    agent {
        kubernetes {
            label 'connectors-se'
            yaml """
apiVersion: v1
kind: Pod
spec:
    containers:
        -
            name: main
            image: 'jenkinsxio/builder-maven:0.1.60'
            command: [cat]
            tty: true
            volumeMounts: [{name: docker, mountPath: /var/run/docker.sock}, {name: m2main, mountPath: /root/.m2/repository}]
            resources: {requests: {memory: 3G, cpu: '2'}, limits: {memory: 8G, cpu: '2'}}
    volumes:
        -
            name: docker
            hostPath: {path: /var/run/docker.sock}
        -
            name: m2main
            hostPath: {path: /tmp/jenkins/tdi/m2}
"""
        }
    }

    environment {
        MAVEN_OPTS = '-Dmaven.artifact.threads=128 -Dorg.slf4j.simpleLogger.showThreadName=true -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss'
        TALEND_REGISTRY = 'registry.datapwn.com'
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
                    // for next concurrent builds
                    sh 'for i in ci_documentation ci_nexus ci_site; do rm -Rf $i; rsync -av . $i; done'
                    // real task
                    sh 'mvn clean install -T1C -PITs -e'
                }
            }
            post {
                always {
                    junit testResults: '*/target/surefire-reports/*.xml', allowEmptyResults: true
                    publishHTML(target: [
                            allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true,
                            reportDir   : 'target/talend-component-kit', reportFiles: 'icon-report.html', reportName: "Icon Report"
                    ])
                    publishHTML(target: [
                            allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true,
                            reportDir   : 'target/talend-component-kit', reportFiles: 'repository-dependency-report.html', reportName: "Dependencies Report"
                    ])
                }
            }
        }
        stage('Post Build Steps') {
            parallel {
                stage('Documentation') {
                    steps {
                        container('main') {
                            withCredentials([
                                    usernamePassword(
                                            credentialsId: 'docker-registry-credentials',
                                            passwordVariable: 'DOCKER_PASSWORD',
                                            usernameVariable: 'DOCKER_LOGIN')
                            ]) {
                                sh """
                     |cd ci_documentation
                     |mvn clean install -DskipTests
                     |chmod +x .jenkins/generate-doc.sh && .jenkins/generate-doc.sh
                     |""".stripMargin()
                            }
                        }
                    }
                    post {
                        always {
                            publishHTML(target: [
                                    allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                                    reportDir   : 'ci_documentation/target/talend-component-kit_documentation/', reportFiles: 'index.html', reportName: "Component Documentation"
                            ])
                        }
                    }
                }
                stage('Site') {
                    steps {
                        container('main') {
                            sh 'cd ci_site && mvn clean site site:stage -T1C -Dmaven.test.failure.ignore=true'
                        }
                    }
                    post {
                        always {
                            publishHTML(target: [
                                    allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                                    reportDir   : 'ci_site/target/staging', reportFiles: 'index.html', reportName: "Maven Site"
                            ])
                        }
                    }
                }
                stage('Nexus') {
                    steps {
                        container('main') {
                            withCredentials([
                                    usernamePassword(
                                            credentialsId: 'nexus-artifact-zl-credentials',
                                            usernameVariable: 'NEXUS_USER',
                                            passwordVariable: 'NEXUS_PASSWORD')
                            ]) {
                                sh "cd ci_nexus && mvn -s .jenkins/settings.xml clean deploy -e -Pdocker -DskipTests -DaltDeploymentRepository=talend.snapshots::default::${deploymentRepository}"
                            }
                        }
                    }
                }
            }
        }
    }
    post {
        success {
            slackSend(color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
        }
        failure {
            slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
        }
    }
}
