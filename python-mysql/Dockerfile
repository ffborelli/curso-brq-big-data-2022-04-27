FROM python:3.9

RUN pip install mysql-connector-python pandas
# copia todos os arquivos da pasta do SO para dentro do container
COPY . .
#executa o script python no CMD
CMD ["miniproject.py"]

ENTRYPOINT ["python"]
