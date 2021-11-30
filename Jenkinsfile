pipeline {
    agent {
        docker {
            label 'docker'
            image 'gradle:7.2-jdk11'
        }
    }
    stages {
        stage('Build') {
            steps {
                echo "Tag $TAG_NAME"
                sh 'gradle --no-daemon clean build'
                archiveArtifacts 'build/libs/*.jar'
            }
        }
        stage('Deploy') {
            environment {
                REPOSILITE = credentials('reposilite')
            }
            when {
                echo "Tag $TAG_NAME"
                tag pattern: "v\\d+\\.\\d+\\.\\d+(-\\w+-\\d+)?", comparator: "REGEXP"
            }
            steps {
                echo "Tag $TAG_NAME"
                script {
                    VERSION = TAG_NAME[1..-1]
                }
                sh "echo Version is ${VERSION}"
                sh "gradle --no-daemon -Pversion=${VERSION} -PreposiliteUsername=${REPOSILITE_USR} -PreposiliteToken=${REPOSILITE_PSW} publish"
            }
        }
    }
}
