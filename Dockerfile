# syntax=docker/dockerfile:1

FROM python:3.8 AS recsys-engine
RUN apt-get update
RUN /usr/local/bin/python -m pip install --upgrade pip
WORKDIR HMRecommenderSystem/
COPY . .
RUN pip3 install -r requirements.txt
RUN pip3 install ../HMRecommenderSystem/
ENV PYTHONPATH=/HMRecommenderSystem
