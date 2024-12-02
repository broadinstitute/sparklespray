docker build . -t sparklesworker
 docker tag sparklesworker us-central1-docker.pkg.dev/cds-docker-containers/docker/sparklesworker:5.0.0-alpha
docker push us-central1-docker.pkg.dev/cds-docker-containers/docker/sparklesworker:5.0.0-alpha
