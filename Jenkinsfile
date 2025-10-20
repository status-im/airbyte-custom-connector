pipeline {
  agent {
    docker {
      label 'linuxcontainer'
      image 'harbor.status.im/infra/ci-build-containers:linux-base-1.0.0'
      args '--volume=/var/run/docker.sock:/var/run/docker.sock ' +
           '--user jenkins'
    }
  }

  options {
    disableRestartFromStage()
    disableConcurrentBuilds()
    /* manage how many builds we keep */
    buildDiscarder(logRotator(
      numToKeepStr: '20',
      daysToKeepStr: '30',
    ))
  }

  parameters {
    string(
      name: 'DOCKER_CRED',
      description: 'Name of Docker Registry credential.',
      defaultValue: params.DOCKER_CRED ?: 'harbor-status-im-robot',
    )
    string(
      name: 'DOCKER_REGISTRY_URL',
      description: 'URL of the Docker Registry',
      defaultValue: params.DOCKER_REGISTRY_URL ?: 'https://harbor.status.im'
    )
  }
  stages {
    stage('Bulding docker images') {
      steps { script {
        def directories=  findFiles().findAll { item -> item.directory && item.name != '.git' }
        for (directory in directories) {
          stage("Building ${directory}"){
            script {
             def metadataVal = readYaml file : "./${directory}/metadata.yaml"
             image_tag = metadataVal['data']['dockerImageTag']
             image_name = metadataVal['data']['dockerRepository']
             echo "Building ${directory} - image name : ${image_name}:${image_tag}"
            }
            script {
              image = docker.build(
                "${image_name}:${image_tag}",
                "./${directory}"
              )
            }
            script {
               withDockerRegistry([
                 credentialsId: params.DOCKER_CRED, url: params.DOCKER_REGISTRY_URL
               ]) {
                 image.push()
              }
            }
          }
        }
      } }
    }
  }
  post {
      always { cleanWs()}
  }
}
