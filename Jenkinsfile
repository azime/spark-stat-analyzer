#!groovy

stage("Unit tests") {
    node('') {

        checkout scm

        wrap([$class: 'AnsiColorBuildWrapper']) {
            sh '''
            cp config.py.dist config.py
            docker pull par-vm232.srv.canaltp.fr:5000/spark-stat-analyser:test
            docker run -e USER_ID=$(id -u) --rm -v $(pwd):/srv/spark-stat-analyzer par-vm232.srv.canaltp.fr:5000/spark-stat-analyser:test sh -c './run_test.sh'
            '''
            junit 'junit.xml'
        }
    }
}