FROM ubuntu:20.04
WORKDIR /peregrine
COPY . . 

# Update package list, install sudo, and then update again
RUN apt-get update && apt-get install -y sudo && apt-get update

# Install the required packages
RUN apt-get install -y g++-10 libunittest++-dev
RUN apt-get install -y openmpi-bin openmpi-dev openmpi-common openmpi-doc libopenmpi-dev

# Execute the 'run.sh' script
CMD ["./scripts/run.sh"]