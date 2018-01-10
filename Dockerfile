FROM ubuntu:trusty

# install system packages
RUN apt-get update && apt-get install -y \
	python \
	python-dev \
	python-pip

RUN pip install \
	elasticsearch \
	protobuf

WORKDIR /app

ADD test-data.tgz /app/

ADD ./src/ /app/

CMD ./main.py
