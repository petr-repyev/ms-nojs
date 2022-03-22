FROM golang:alpine

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
WORKDIR /build

# Download dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY *.yml *.go ./

# Build
RUN go build -o main .

# Result
WORKDIR /dist
RUN cp /build/config.yml .
RUN cp /build/main .

# local ips

CMD ["/dist/main"]
