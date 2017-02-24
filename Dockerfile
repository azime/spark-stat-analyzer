FROM gettyimages/spark:2.1.0-hadoop-2.7

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install \
        libpq5 \
        zip \
        && \
    rm -rf /var/lib/apt/lists/*

COPY . /srv/spark-stat-analyzer

WORKDIR /srv/spark-stat-analyzer

RUN set -xe && \
    buildDeps="libpq-dev python3-dev build-essential" && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install $buildDeps && \
    pip3 install -r requirements.txt && \
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false -o APT::AutoRemove::SuggestsImportant=false $buildDeps && \
    rm -rf /var/lib/apt/lists/*

RUN cp config.py.docker config.py && rm config.py.dist && rm config.py.docker
RUN zip -r spark-stat-analyzer.zip analyzers includes
RUN cp /usr/spark-2.1.0/conf/log4j.properties.template /usr/spark-2.1.0/conf/log4j.properties
RUN sed -i 's/INFO, console/WARN, console/g' /usr/spark-2.1.0/conf/log4j.properties

CMD ["bash"]
