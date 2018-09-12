node ('nimble-jenkins-slave') {
    stage('Download Latest') {
        git(url: 'https://github.com/nimble-platform/Delegate-service.git', branch: 'master')
    }

    stage ('Build docker image') {
        sh 'mvn -Dmaven.test.skip=true clean install'
        sh 'docker build -t nimbleplatform/Delegate-service:${BUILD_NUMBER} .'
        sh 'sleep 5'
    }

    stage ('Push docker image') {
        withDockerRegistry([credentialsId: 'NimbleDocker']) {
            sh 'docker push nimbleplatform/Delegate-service:${BUILD_NUMBER}'
        }
    }

    stage ('Deploy') {
        sh ''' sed -i 's/IMAGE_TAG/'"$BUILD_NUMBER"'/g' kubernetes/deploy.yaml '''
        sh 'kubectl apply -f kubernetes/deploy.yaml -n prod --validate=false'
        sh 'kubectl apply -f kubernetes/svc.yaml -n prod --validate=false'
    }

    stage ('Print-deploy logs') {
        sh 'sleep 60'
        sh 'kubectl -n prod logs deploy/Delegate-service -c Delegate-service'
    }
}