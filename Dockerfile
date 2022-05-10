# partindo da imagem ubuntu
FROM ubuntu:latest
# RUN serve para rodar um comado shell script na criacao da imagem docker
RUN apt-get update && apt-get install python3-pip -y \ 
    && apt-get install nano -y

RUN mkdir /nova-pasta

# ao iniciar o container, o mesmo apresenta o console
ENTRYPOINT ["/bin/bash"]

# docker build -t minha-imagem .
#docker run --name mi -it minha-imagem