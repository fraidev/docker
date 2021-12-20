FROM golang:1.17-alpine

WORKDIR /app

COPY *.go ./

RUN go build ./main.go

CMD [ "/main" ]