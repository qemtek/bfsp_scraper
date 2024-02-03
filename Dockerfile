FROM python:3.8-slim-buster

RUN apt-get update
RUN apt-get update && apt-get -y install gcc
RUN apt-get install --reinstall build-essential -y
RUN pip install --upgrade pip

ADD requirements.txt /

RUN mkdir -p /bfsp_scraper
COPY bfsp_scraper /bfsp_scraper

RUN pip3 install -r requirements.txt

ENV COUNTRIES='uk,ire,usa,aus'
ENV TYPES='win,place'
ENV S3_BUCKET='betfair-sp'
ENV AWS_GLUE_DB='finish-time-predict'
ENV PROJECT_DIR=/bfsp_scraper
ENV PYTHONPATH=/

CMD ["python", "bfsp_scraper/get_yesterdays_data.py"]