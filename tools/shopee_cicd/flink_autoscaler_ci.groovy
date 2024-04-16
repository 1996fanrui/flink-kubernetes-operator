def TAR_NAME

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

                    sh "rm -f flink-autoscaler-standalone/src/main/resources/log4j2.properties"

                    sh "mvn -U clean install -DskipTests -pl flink-autoscaler,flink-autoscaler-plugin-jdbc,flink-autoscaler-standalone -s tools/maven/settings.xml"
                    sh "echo 'Maven BUILD SUCCESS'"
                }
            }
        }
        stage("generate tar package") {
            steps {
                script {
                    TAR_NAME = sh (
                            script: "echo ${params.TAG_NAME}.tar.gz ",
                            returnStdout: true
                    ).trim()

                    sh "echo 'Start generate the tar.'"

                    sh "rm -rf ./shopee-ci/dist/bin"
                    sh "rm -rf ./shopee-ci/dist/lib"
                    sh "mkdir -p ./shopee-ci/dist/bin"
                    sh "mkdir -p ./shopee-ci/dist/lib"
                    sh "rm -rf ./shopee-ci/tar"
                    sh "mkdir -p ./shopee-ci/tar"

                    sh "cp ./flink-autoscaler-standalone/target/flink-autoscaler-standalone-*.jar ./shopee-ci/dist/lib/flink-autoscaler-standalone.jar"
                    sh "cp ./flink-autoscaler-standalone/src/main/bin/start-fa.sh ./shopee-ci/dist/bin/start-fa.sh"

                    sh "cd ./shopee-ci && tar -zcvf ./tar/${TAR_NAME} ./dist"

                    sh "echo 'Generate TAR SUCCESS'"
                }
            }
        }
        stage('Upload Nexus') {
            steps {
                withCredentials([usernamePassword(credentialsId: "flink-nexus-secret", passwordVariable: "password", usernameVariable: "username")]) {
                    sh "NEXUS_SERVER=nexus-repo.data-infra.shopee.io NEXUS_USER=$username NEXUS_PASSWD=$password di-nexus-upload ./shopee-ci/tar/${TAR_NAME} Flink/flink-autoscaler/"
                }
            }
        }
    }
}
