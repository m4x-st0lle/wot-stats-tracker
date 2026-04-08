FROM apache/spark:3.5.0
WORKDIR /opt/spark/work-dir

COPY requirements.txt .

USER root

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt


COPY ./src ./src

USER spark