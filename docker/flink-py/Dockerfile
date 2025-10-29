FROM flink:1.17.1-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.17.1 httpx==0.27.2 requests==2.31.0
