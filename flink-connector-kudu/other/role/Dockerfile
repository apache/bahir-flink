FROM bitnami/minideb:jessie
MAINTAINER eskabetxe

RUN set -x \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
	   bzip2 unzip xz-utils wget \
    && cd /etc/apt/sources.list.d \
    && wget -qO - http://archive.cloudera.com/kudu/debian/jessie/amd64/kudu/archive.key | apt-key add - \
    && wget http://archive.cloudera.com/kudu/debian/jessie/amd64/kudu/cloudera.list \
    && apt-get update \
    && apt-get install  --no-install-recommends -y \
       kudu kudu-master kudu-tserver libkuduclient0 libkuduclient-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get autoclean

VOLUME /var/lib/kudu/master /var/lib/kudu/tserver

COPY docker-entrypoint.sh /
RUN chmod a+x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 8050 8051 7050 7051
#CMD ["help"]
