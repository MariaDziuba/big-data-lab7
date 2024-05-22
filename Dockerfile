FROM apache/spark:3.4.1-scala2.12-java11-python3-ubuntu

WORKDIR /app
USER root

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENV SBT_VERSION="1.9.4"

ADD mysql-connector-j-8.4.0.jar /opt/spark/jars/

RUN curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt

RUN ln -sf $(which python3) /usr/bin/python && \
    ln -sf $(which pip3) /usr/bin/pip

RUN apt-get update
RUN apt-get install -y gcc python3-dev 
RUN pip install --upgrade pip setuptools

COPY . /app

RUN cd datamart && bash build.sh && cd ..

RUN pip install --no-cache-dir -r requirements.txt