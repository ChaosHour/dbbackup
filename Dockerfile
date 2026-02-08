# Multi-stage build for minimal image size
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

# Build arguments for cross-compilation
ARG TARGETOS
ARG TARGETARCH

# Install build dependencies
RUN apk add --no-cache git make

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary with cross-compilation support
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -a -installsuffix cgo -ldflags="-w -s" -o dbbackup .

# Final stage - minimal runtime image
# Using pinned version 3.19 which has better QEMU compatibility
FROM alpine:3.19

# Install database client tools
# Split into separate commands for better QEMU compatibility
RUN apk add --no-cache postgresql-client
RUN apk add --no-cache mysql-client
RUN apk add --no-cache mariadb-client
RUN apk add --no-cache pigz pv
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 dbbackup && \
    adduser -D -u 1000 -G dbbackup dbbackup

# Copy binary from builder
COPY --from=builder /build/dbbackup /usr/local/bin/dbbackup
RUN chmod +x /usr/local/bin/dbbackup

# Create backup directory
RUN mkdir -p /backups && chown dbbackup:dbbackup /backups

# Set working directory
WORKDIR /backups

# Switch to non-root user
USER dbbackup

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/dbbackup"]

# Default command shows help
CMD ["--help"]

# Labels
LABEL maintainer="UUXO"
LABEL version="6.0"
LABEL description="Professional database backup tool for PostgreSQL, MySQL, and MariaDB"
