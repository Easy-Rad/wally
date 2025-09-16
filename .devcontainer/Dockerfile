FROM mcr.microsoft.com/devcontainers/python:1-3-bookworm
COPY corporate.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/corporate.crt