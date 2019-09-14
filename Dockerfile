FROM python:3
MAINTAINER Eduard Asriyan <ed-asriyan@protonmail.com>

WORKDIR /application

ADD requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ADD server ./server
ADD config/__init__.py ./config/__init__.py
ADD start.py .

CMD python3 ./start.py