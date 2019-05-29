def slackChannel = 'components-ci'

def PRODUCTION_DEPLOYMENT_REPOSITORY = "TalendOpenSourceSnapshot"

def branchName = env.BRANCH_NAME
if (BRANCH_NAME.startsWith("PR-")) {
    branchName = env.CHANGE_BRANCH
}

def escapedBranch = branchName.toLowerCase().replaceAll("/", "_")
def deploymentSuffix = env.BRANCH_NAME == "master" ? "${PRODUCTION_DEPLOYMENT_REPOSITORY}" : ("dev_branch_snapshots/branch_${escapedBranch}")
def m2 = "/tmp/jenkins/tdi/m2/${deploymentSuffix}"
def talendOssRepositoryArg = env.BRANCH_NAME == "master" ? "" : ("-Dtalend_oss_snapshots=https://nexus-smart-branch.datapwn.com/nexus/content/repositories/${deploymentSuffix}")

def calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

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
            image: 'khabali/jenkins-java-build-container:latest'
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
            hostPath: { path: ${m2} }
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

    parameters {
        booleanParam(name: 'PUSH_TO_XTM', defaultValue: false, description: 'Export the project i18n resources to Xtm to be translated. This action can be performed from master or maintenance branches only.')
        booleanParam(name: 'DEPLOY_FROM_XTM', defaultValue: false, description: 'Download and deploy i18n resources from Xtm to nexus for this branch.')
    }

    stages {
        stage('Run maven') {
            when {
                expression { params.PUSH_TO_XTM == false && params.DEPLOY_FROM_XTM == false }
            }
            steps {
                container('main') {
                    // for next concurrent builds
                    sh 'for i in ci_documentation ci_nexus ci_site; do rm -Rf $i; rsync -av . $i; done'
                    // real task
                    withCredentials([
                            usernamePassword(
                                    credentialsId: 'nexus-artifact-zl-credentials',
                                    usernameVariable: 'NEXUS_USER',
                                    passwordVariable: 'NEXUS_PASSWORD')
                            , usernamePassword(
                                credentialsId: 'onedrive-integration',
                                usernameVariable: 'ONEDRIVE_INTEGRATION_USER',
                                passwordVariable: 'ONEDRIVE_INTEGRATION_PASSWORD')
                    ]) {
                        script {
                            sh "mvn -U -B -s .jenkins/settings.xml clean install -PITs -Dtalend.maven.decrypter.m2.location=${env.WORKSPACE}/.jenkins/ -e ${talendOssRepositoryArg}"
                        }
                    }
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
            when {
                expression { params.PUSH_TO_XTM == false && params.DEPLOY_FROM_XTM == false }
            }
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
                     |mvn -U -B -s .jenkins/settings.xml clean install -DskipTests
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
                            sh 'cd ci_site && mvn -U -B -s .jenkins/settings.xml clean site site:stage -Dmaven.test.failure.ignore=true'
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
                                sh "cd ci_nexus && mvn -U -B -s .jenkins/settings.xml clean deploy -e -Pdocker -DskipTests ${talendOssRepositoryArg}"
                            }
                        }
                    }
                }
            }
        }
        stage('Push to Xtm') {
            when {
                anyOf{
                    expression { params.PUSH_TO_XTM == true }
//                    allOf{
//                        triggeredBy 'TimerTrigger'
//                        expression {
//                            (calendar.get(Calendar.WEEK_OF_MONTH) == 2 ||  calendar.get(Calendar.WEEK_OF_MONTH) == 4) && calendar.get(Calendar.DAY_OF_WEEK) == Calendar.THURSDAY
//                        }
//                    }
                }
                anyOf {
                    branch 'master'
                    expression { BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
                container('main') {
                    withCredentials([
                            usernamePassword(
                                    credentialsId: 'nexus-artifact-zl-credentials',
                                    usernameVariable: 'NEXUS_USER',
                                    passwordVariable: 'NEXUS_PASSWORD'),
                            usernamePassword(
                                    credentialsId: 'xtm-credentials',
                                    usernameVariable: 'XTM_USER',
                                    passwordVariable: 'XTM_TOKEN')
                    ]) {
                        script {
                            sh "mvn -e -B -s .jenkins/settings.xml clean package -pl . -Pi18n-export"
                        }
                    }
                }
            }
        }
        stage('Deploy from Xtm') {
            when {
                expression { params.DEPLOY_FROM_XTM == true }
                anyOf {
                    branch 'master'
                    expression { BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
                container('main') {
                    withCredentials([
                            usernamePassword(
                                    credentialsId: 'nexus-artifact-zl-credentials',
                                    usernameVariable: 'NEXUS_USER',
                                    passwordVariable: 'NEXUS_PASSWORD'),
                            usernamePassword(
                                    credentialsId: 'xtm-credentials',
                                    usernameVariable: 'XTM_USER',
                                    passwordVariable: 'XTM_TOKEN'),
                            usernamePassword(
                                    credentialsId: 'github-i18n-product',
                                    usernameVariable: 'GITHUB_LOGIN',
                                    passwordVariable: 'GITHUB_TOKEN')
                    ]) {
                        script {
                            sh "mvn -e -B -s .jenkins/settings.xml clean package -pl . -Pi18n-deploy"
                            sh "cd tmp/repository && mvn -s ../../.jenkins/settings.xml clean deploy"
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
