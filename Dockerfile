FROM python:3.10-slim-bullseye
ARG DEBIAN_FRONTEND=noninteractive
WORKDIR /code
RUN find . -type d -name __pycache__ -exec rm -r {} \+
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY . /code
