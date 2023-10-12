FROM ubuntu:20.04
WORKDIR /peregrine
COPY . . 

RUN apt-get --yes -qq update \
 && apt-get --yes -qq upgrade \
 && apt-get --yes -qq install \
                      cmake \
                      g++ \
                      gcc \
                      gfortran \
                      libblas-dev \
                      liblapack-dev \
                      libopenmpi-dev \
 && apt-get --yes -qq clean 

CMD [ "/bin/bash" ]