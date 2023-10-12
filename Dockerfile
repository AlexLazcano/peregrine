FROM ubuntu:20.04
WORKDIR /peregrine
COPY . . 
ENV DEBIAN_FRONTEND=noninteractive 
RUN apt-get --yes -qq update \
 && apt-get --yes -qq upgrade \
 && apt-get --yes -qq install \
                      cmake \
                      g++ \
                      gcc \
                      libopenmpi-dev \
 && apt-get --yes -qq clean 
