FROM bde2020/spark-submit:2.4.5-hadoop2.7

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py

COPY submit.sh /

RUN apk add --no-cache build-base g++ python3-dev \
      cyrus-sasl-dev cyrus-sasl-plain

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

CMD ["/bin/bash", "/submit.sh"]
