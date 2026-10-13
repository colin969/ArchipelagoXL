FROM golang:1.26-trixie

WORKDIR /apx

RUN go install github.com/air-verse/air@latest

COPY ./apx/go.mod ./apx/go.sum ./
RUN go mod download

ENTRYPOINT ["air"]
