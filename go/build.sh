IMAGE_NAME=us-central1-docker.pkg.dev/cds-docker-containers/docker/sparklesworker:5.0.0-alpha3
docker build . -t sparklesworker
docker tag sparklesworker ${IMAGE_NAME}
docker push ${IMAGE_NAME}
