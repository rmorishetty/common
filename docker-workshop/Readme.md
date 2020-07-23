## section 1:
topics: build, run, -t, images, FROM, CMD, layers
> docker build . -t docker_workshop_1

> docker run docker_workshop_1

> docker images

## section 2:
topics: ENV, -e, CMD
> docker build . -t docker_workshop_2

> docker run -e 'ATRIBUTO=KAPO' docker_workshop_2

> docker images

## section 3:
topics: COPY, ENTRYPOINT, CMD
> docker build . -t docker_workshop_3

> docker run docker_workshop_3

> docker images

## section 4:
topics: WORKDIR, RUN, -f file, taging, rmi
> docker build . -f Dockerfile_bis -t docker_workshop_3:latest

> docker run docker_workshop_4

> docker build . -t docker_workshop_4:bis

> docker run docker_workshop_4:bis

> docker images

> docker tag docker_workshop_4:bis docker_workshop_4:good

> docker rmi docker_workshop_4:bis

> docker run docker_workshop_4:good


## section 5:
topics: first api, --name my_flask_app, ps, exec, kill, stop, port 

> docker build . -t docker_workshop_5

> docker run --name my_flask_app docker_workshop_5

> docker ps 

> docker exec -ti my_flask_app bash

> docker kill my_flask_app 

> docker stop my_flask_app 

> docker run --name my_flask_app docker_workshop_5

> docker rm my_flask_app

> curl 127.0.0.1:5000

> docker exec -ti my_flask_app bash

> curl 127.0.0.1:5000

> docker run --name my_flask_app_exp_port -p 0.0.0.0:5000:5000  docker_workshop_5

> curl 127.0.0.1:5000


## section 6:
topics: volumes, hot reload, run as daemon, logs

> docker build . -t docker_workshop_6

> docker run --name my_flask_app_with_reload -p 0.0.0.0:5000:5000 -v $PWD/app:/app docker_workshop_6

> curl 127.0.0.1:5000

> #make some change on the code, and view what is happening

> curl 127.0.0.1:5000

> docker rm -f my_flask_app_with_reload

> docker run -d --name my_flask_app_with_reload -p 0.0.0.0:5000:5000 -v $PWD/app:/app docker_workshop_6

> docker logs -f my_flask_app_with_reload


## section 7
topics: push and pull

> #be sure you have a free account on https://hub.docker.com/

> docker login

> docker tag docker_workshop_6 pabloncio/docker_workshop:1.0.0 

> docker push

> docker rm -f  $(docker ps -aq)

> rmi -f $(docker images -aq)

> docker pull pabloncio/docker_workshop:1.0.0
