FROM python:3-alpine AS base
COPY certificates/corporate.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
RUN adduser -D app
USER app
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/corporate.crt

FROM base AS service_ps360
WORKDIR /app
COPY --chown=app:app ps360/* .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
CMD [ "python", "-u", "-m", "ps360"]

FROM base AS service_xmpp
WORKDIR /app
COPY --chown=app:app xmpp/* .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
CMD [ "python", "-u", "-m", "xmpp"]

FROM base AS service_wally
WORKDIR /app
COPY --chown=app:app wally.py requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
CMD [ "python", "-u", "-m", "wally"]