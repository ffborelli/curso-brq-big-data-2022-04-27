FROM bde2020/hive:2.3.2-postgresql-metastore

COPY ./hive_setup.sql /usr/local/hive_setup.sql
COPY startup.sh /usr/local/bin/

 RUN sed -i 's/\r$//' /usr/local/bin/startup.sh  && \  
        chmod +x /usr/local/bin/startup.sh

#RUN chmod +x /usr/local/bin/startup.sh

ENTRYPOINT ["entrypoint.sh"]
CMD startup.sh
