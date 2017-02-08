#!groovy

stage("Unit tests") {
    node('') {

        checkout scm

        wrap([$class: 'AnsiColorBuildWrapper']) {
            sh '''
            cp config.py.dist config.py
            docker run -v $(pwd):/srv/spark-stat-analyzer par-vm232.srv.canaltp.fr:5000/stat-analyser:test
            '''
        }
    }
}