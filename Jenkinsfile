def slackChannel = 'components-ci'

def nexusCredentials = usernamePassword(
	credentialsId: 'nexus-artifact-zl-credentials',
    usernameVariable: 'NEXUS_USER',
    passwordVariable: 'NEXUS_PASSWORD')
def gitCredentials = usernamePassword(
	credentialsId: 'github-credentials',
    usernameVariable: 'GITHUB_LOGIN',
    passwordVariable: 'GITHUB_TOKEN')
def dockerCredentials = usernamePassword(
	credentialsId: 'docker-registry-credentials',
    passwordVariable: 'DOCKER_PASSWORD',
    usernameVariable: 'DOCKER_LOGIN')


def PRODUCTION_DEPLOYMENT_REPOSITORY = "TalendOpenSourceSnapshot"

def branchName = env.BRANCH_NAME
if (BRANCH_NAME.startsWith("PR-")) {
    branchName = env.CHANGE_BRANCH
}

def escapedBranch = branchName.toLowerCase().replaceAll("/", "_")
def deploymentSuffix = (env.BRANCH_NAME == "master" || env.BRANCH_NAME.startsWith("maintenance/")) ? "${PRODUCTION_DEPLOYMENT_REPOSITORY}" : ("dev_branch_snapshots/branch_${escapedBranch}")

def m2 = "/tmp/jenkins/tdi/m2/${deploymentSuffix}"
def talendOssRepositoryArg = (env.BRANCH_NAME == "master" || env.BRANCH_NAME.startsWith("maintenance/")) ? "" : ("-Dtalend_oss_snapshots=https://nexus-smart-branch.datapwn.com/nexus/content/repositories/${deploymentSuffix}")

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
        buildDiscarder(logRotator(artifactNumToKeepStr: '5', numToKeepStr: (env.BRANCH_NAME == 'master' || env.BRANCH_NAME.startsWith('maintenance/')) ? '10' : '2'))
        timeout(time: 60, unit: 'MINUTES')
        skipStagesAfterUnstable()
    }

    triggers {
        cron(env.BRANCH_NAME == "master" ? "@daily" : "")
    }

    parameters {
        choice(name: 'Action', 
               choices: [ 'STANDARD', 'PUSH_TO_XTM', 'DEPLOY_FROM_XTM', 'RELEASE' ],
               description: 'Kind of running : \nSTANDARD (default), normal building\n PUSH_TO_XTM : Export the project i18n resources to Xtm to be translated. This action can be performed from master or maintenance branches only. \nDEPLOY_FROM_XTM: Download and deploy i18n resources from Xtm to nexus for this branch.\nRELEASE : build release')
    }

    stages {
        stage('Run maven') {
            when {
                expression { params.Action == 'STANDARD' }
            }
            steps {
                container('main') {
                	script {
	                    // for next concurrent builds
	                    sh 'for i in ci_documentation ci_nexus ci_site; do rm -Rf $i; rsync -av . $i; done'
	                    // real task
	
	                    withCredentials([nexusCredentials, gitCredentials]) {
	                        sh "mvn -U -B -s .jenkins/settings.xml clean install -PITs -e ${talendOssRepositoryArg}"
	
							// On pull request => add spotbugs comments on PR for ecach changed file.                        
	                        if (env.CHANGE_ID) {
		                     	sh """
		                     	    |mvn --batch-mode -Dspotbugs.effort=max com.github.spotbugs:spotbugs-maven-plugin:3.1.12:spotbugs  -s .jenkins/settings.xml
		                     	    |current_rep=`pwd` 
		                     	    |cd .jenkins/prvalidator
		                     	    |mvn package -DskipTests --batch-mode  -s ../settings.xml
		                     	    |cd target
		                     	    |java -jar prvalidator-1.0.jar ${GITHUB_LOGIN} ${GITHUB_TOKEN} ${env.CHANGE_ID} ${env.GIT_COMMIT} connectors-se \${current_rep}
		                        """.stripMargin()   
		                    }        
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
                expression { params.Action == 'STANDARD' }
            }
            parallel {
                stage('Documentation') {
                    steps {
                        container('main') {
                            withCredentials([dockerCredentials]) {
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
                            withCredentials([nexusCredentials]) {
                                sh "cd ci_nexus && mvn -U -B -s .jenkins/settings.xml clean deploy -e -Pdocker -DskipTests ${talendOssRepositoryArg}"
                            }
                        }
                    }
                }
            }
        }
        stage('Push to Xtm') {
            when {
                anyOf {
                    expression { params.Action == 'PUSH_TO_XTM' }
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
                    withCredentials([nexusCredentials,
                            string(
                                    credentialsId: 'xtm-token',
                                    variable: 'XTM_TOKEN')
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
                expression { params.Action == 'DEPLOY_FROM_XTM' }
                anyOf {
                    branch 'master'
                    expression { BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
                container('main') {
                    withCredentials([nexusCredentials,
                            string(
                                    credentialsId: 'xtm-token',
                                    variable: 'XTM_TOKEN'),
                            gitCredentials ]) {
                        script {
                            sh "mvn -e -B -s .jenkins/settings.xml clean package -pl . -Pi18n-deploy"
                            sh "cd tmp/repository && mvn -s ../../.jenkins/settings.xml clean deploy"
                        }
                    }
                }
            }
        }
        stage('Release') {
			when {
				expression { params.Action == 'RELEASE' }
                anyOf {
                    branch 'master'
                    expression { BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
            	withCredentials([gitCredentials, nexusCredentials]) {
					container('main') {
                		
						sh """
						    mvn -B -s .jenkins/settings.xml release:clean release:prepare
						    if [[ \$? -eq 0 ]] ; then
						        PROJECT_VERSION=\$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
						    	mvn -B -s .jenkins/settings.xml -Darguments='-Dmaven.javadoc.skip=true' release:perform
						    	git push origin release/\${PROJECT_VERSION}
						    	git push
						    fi
						"""
						
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
