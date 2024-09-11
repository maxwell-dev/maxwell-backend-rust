# syntax=docker/dockerfile:1
# escape=\

FROM ubuntu:22.04
LABEL maintainer="maxwell-dev <https://github.com/maxwell-dev>"
SHELL ["/bin/bash", "-c"]

ARG uid

RUN adduser --disabled-password --no-create-home --gecos "" --uid ${uid:-20000} maxwell

WORKDIR /maxwell-backend
RUN mkdir -p log
COPY config/config.template.toml config/config.toml
COPY config/log4rs.template.yaml config/log4rs.yaml
COPY target/release/maxwell-backend .
RUN chown -R maxwell:maxwell .

USER maxwell
CMD ["./maxwell-backend"]
