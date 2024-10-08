FROM python:3.12-slim

RUN apt-get update
RUN apt-get install make

COPY . /opt/servicelayer
WORKDIR /opt/servicelayer
RUN make dev

CMD /bin/bash
