#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
