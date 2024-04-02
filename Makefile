all: sidecar

sidecar:
	CGO_ENABLED=0 GOOS=linux go build -o bin/slurm-sd cmd/main.go
