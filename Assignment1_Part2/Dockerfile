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
RUN pip install ipython
RUN pip install boto3
RUN pip install -U boto
RUN pip install bokeh
RUN pip install scipy

ADD run.sh run.sh
ADD 2306175529.txt 2306175529.txt
ADD FL_230617_12832.csv FL_230617_12832.csv
ADD FL_230617_12832_clean.csv FL_230617_12832_clean.csv
ADD configWrangle.json configWrangle.json
ADD wrangle.py wrangle.py
ADD Makefile Makefile
ADD 998702.csv 998702.csv
ADD cleanDataEDA.ipynb cleanDataEDA.ipynb

ENTRYPOINT ["bash","run.sh"]