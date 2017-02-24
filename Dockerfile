FROM gettyimages/spark:2.1.0-hadoop-2.7

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install \
        libpq-dev \
        python3-dev \
        build-essential \
        && \
    rm -rf /var/lib/apt/lists/*

COPY . /srv/spark-stat-analyzer

WORKDIR /srv/spark-stat-analyzer

RUN pip3 install -r requirements.txt

RUN cp config.py.docker config.py && rm config.py.dist && rm config.py.docker

CMD ["bash"]
