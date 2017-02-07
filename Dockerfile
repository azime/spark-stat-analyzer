FROM gettyimages/spark:2.0.2-hadoop-2.7

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install \
        libpq-dev \
        python-pip \
        python-dev \
        && \
    rm -rf /var/lib/apt/lists/*

COPY . /srv/spark-stat-analyzer

WORKDIR /srv/spark-stat-analyzer

RUN pip2 install -r requirements.txt

RUN cp config.py.docker config.py && rm config.py.dist && rm config.py.docker

CMD ["bash"]
