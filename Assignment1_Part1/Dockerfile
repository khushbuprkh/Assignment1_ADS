FROM ubuntu:latest

RUN apt-get update -q && apt-get install -yqq \
    apt-utils \
    git \
    vim \
    nano \
    ssh \
    gcc \
    make \
    build-essential \
    libkrb5-dev \
    sudo 
	
RUN apt-get install -y python python-dev python-distribute python-pip
RUN pip install --upgrade pip
RUN pip install numpy
RUN pip install pandas
RUN pip install matplotlib
RUN pip install ipython
RUN pip install boto3
RUN pip install -U boto
RUN pip install bokeh
RUN pip install jupyter

ADD FL_12832.csv FL_12832.csv
ADD FL_230617_12832.csv FL_230617_12832.csv
ADD 2306173953.txt 2306173953.txt
ADD 998702.csv 998702.csv
ADD run.sh run.sh
ADD config.json config.json
ADD dataingestion.py dataingestion.py
ADD Makefile Makefile
ADD rawDataEDA.ipynb rawDataEDA.ipynb

ENTRYPOINT ["bash","run.sh"]