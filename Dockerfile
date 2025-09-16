FROM python:3-alpine
COPY certificates/corporate.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
RUN adduser -D app
USER app
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/corporate.crt
WORKDIR /app
COPY --chown=app:app wally.py xmpp.py ps360.py requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
CMD [ "python", "-u", "-m", "wally"]