def slackChannel = 'components-ci'
def version = 'will be replaced'
def image = 'will be replaced'

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

    parameters {
        string(
                name: 'COMPONENT_SERVER_IMAGE_VERSION',
                defaultValue: '1.1.2',
                description: 'The Component Server docker image tag')
        booleanParam(
	        name: 'PUSH_DOCKER_IMAGE',
	        defaultValue: false,
		description: 'If dev branch, push generated docker image to datapwn (it is always done for master).')
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
                    sh 'for i in ci_docker ci_nexus ci_site; do rm -Rf $i; rsync -av . $i; done'
                    // real task
                    sh 'mvn clean install -T1C -Pdocker -PITs -e'
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
                stage('Docker') {
                    when {
		        expression { sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim() == 'master' || params.PUSH_DOCKER_IMAGE == true }
                    }
                    steps {
                        container('main') {
                            withCredentials([
                                    usernamePassword(
                                            credentialsId: 'docker-registry-credentials',
                                            passwordVariable: 'DOCKER_PASSWORD',
                                            usernameVariable: 'DOCKER_LOGIN')
                            ]) {
                                sh """
                     |cd ci_docker
                     |mvn clean install -Pdocker -DskipTests
                     |chmod +x ./connectors-se-docker/src/main/scripts/docker/*.sh
                     |revision=`git rev-parse --abbrev-ref HEAD | tr / _`
                     |./connectors-se-docker/src/main/scripts/docker/all.sh \$revision
                     |
                     |# collect doc
                     |chmod +x .jenkins/generate-doc.sh && .jenkins/generate-doc.sh
                     |""".stripMargin()
                            }
                        }
                    }
                    post {
                        always {
                            publishHTML(target: [
                                    allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                                    reportDir   : 'ci_docker/connectors-se-docker/target', includes: 'docker.html', reportFiles: 'docker.html', reportName: "Docker Images"
                            ])
                            publishHTML(target: [
                                    allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                                    reportDir   : 'ci_docker/target/talend-component-kit_documentation/', reportFiles: 'index.html', reportName: "Component Documentation"
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
                                sh "cd ci_nexus && mvn -s .jenkins/settings.xml clean deploy -e -DskipTests -DaltDeploymentRepository=talend.snapshots::default::${deploymentRepository}"
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
            script {
                if (params.COMPONENT_SERVER_IMAGE_VERSION) {
                    println "Launching Connectors EE build with component server docker image >${params.COMPONENT_SERVER_IMAGE_VERSION}<"
                    build job: '/connectors-ee/master',
                            parameters: [string(name: 'COMPONENT_SERVER_IMAGE_VERSION', value: "${params.COMPONENT_SERVER_IMAGE_VERSION}")],
                            wait: false, propagate: false
                }
            }
        }
        failure {
            slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
        }
    }
}
