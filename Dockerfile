FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -qq -y update \
    && apt-get -qq -y install python3-pip \
    pkg-config libicu-dev \
    && apt-get -qq -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install --no-binary=:pyicu: pyicu

ENV LANG='en_US.UTF-8'

COPY . /opt/servicelayer
WORKDIR /opt/servicelayer
RUN make dev

CMD /bin/bash
