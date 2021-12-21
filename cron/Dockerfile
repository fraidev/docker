# FROM golang:1.17-alpine

# WORKDIR /app

# COPY go.mod ./
# RUN go mod download

# COPY *.go ./
# RUN go build -o /main

# CMD [ "/main" ]


FROM tezos/tezos:master
RUN sudo apk add curl lz4 xz jq
COPY cron.sh /
ENTRYPOINT ["/cron.sh"]
CMD []
