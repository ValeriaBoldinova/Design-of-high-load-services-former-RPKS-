FROM flink:1.18.1-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*


ENV PYFLINK_PYTHON=/usr/bin/python3

RUN pip3 install --no-cache-dir \
    apache-flink==1.18.0 \
    psycopg2-binary

WORKDIR /app
COPY job.py .

CMD ["python3", "job.py"]
