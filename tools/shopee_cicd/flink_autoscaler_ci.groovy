pipeline {
    agent {
        kubernetes {
            yaml """
                apiVersion: v1
                kind: Pod
                spec:
                  containers:
                  - name: maven
                    image: "maven:3.8.4-openjdk-17"
                    imagePullPolicy: Always
                    command: ["cat"]
                    tty: true
                    volumeMounts: # add volumes for .m2
                    - mountPath: "/root/.m2"
                      name: "volume-0"
                      readOnly: false
            """
        }
    }
    stages {
        stage("Pull SourceCode") {
            steps {
                checkout(
                        scm: [
                                $class: 'GitSCM',
                                userRemoteConfigs: [
                                        [
                                                credentialsId: 'jenkins-gitlab-credential',
                                                url: 'gitlab@git.garena.com:shopee/data-infra/realtime-flink/flink-kubernetes-operator.git'
                                        ]
                                ],
                                branches: [[name: "${params.TAG_NAME}"]],
                                extensions: [],
                                gitTool: 'git'
                        ],
                        poll: false,
                        changelog: false
                )
            }
        }
        stage("Maven build") {
            steps {
                container('maven') {
                    sh "echo 'Start Maven BUILD'"
                    sh "set -o errexit"
                    sh "set -o nounset"
                    sh "set -o xtrace"

                    sh "mvn -U clean install -DskipTests -pl flink-autoscaler,flink-autoscaler-plugin-jdbc,flink-autoscaler-standalone -s tools/maven/settings.xml"
                    sh "echo 'Maven BUILD SUCCESS'"
                }
            }
        }
        stage("generate jar package") {
            steps {
                script {
                    sh "echo 'Start generate the jar.'"

                    sh "rm -rf ./shopee-dist"
                    sh "mkdir ./shopee-dist"
                    sh "mv ./flink-autoscaler-standalone/target/flink-autoscaler-standalone-*.jar ./shopee-dist/flink-autoscaler-standalone.jar"

                    sh "echo 'Generate JAR SUCCESS'"
                }
            }
        }
        stage('Upload Nexus') {
            steps {
                withCredentials([usernamePassword(credentialsId: "flink-nexus-secret", passwordVariable: "password", usernameVariable: "username")]) {
                    sh "NEXUS_SERVER=nexus-repo.data-infra.shopee.io NEXUS_USER=$username NEXUS_PASSWD=$password di-nexus-upload ./shopee-dist/flink-autoscaler-standalone.jar Flink/flink-autoscaler/${params.TAG_NAME}"
                }
            }
        }
    }
}
