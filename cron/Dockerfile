FROM tezos/tezos:master
RUN sudo apk add curl lz4 xz jq
COPY cron.sh /
ENTRYPOINT ["/cron.sh"]
CMD []
