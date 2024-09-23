package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	logruslogger "github.com/virtual-kubelet/virtual-kubelet/log/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	slurm "github.com/intertwin-eu/interlink-slurm-plugin/pkg/slurm"

	"github.com/virtual-kubelet/virtual-kubelet/trace"
	"github.com/virtual-kubelet/virtual-kubelet/trace/opentelemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func initProvider(ctx context.Context) (func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceName("InterLink-SLURM-plugin"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	otlpEndpoint := os.Getenv("TELEMETRY_ENDPOINT")

	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4317"
	}

	fmt.Println("TELEMETRY_ENDPOINT: ", otlpEndpoint)

	conn := &grpc.ClientConn{}
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	conn, err = grpc.DialContext(ctx, otlpEndpoint, grpc.WithTransportCredentials(creds), grpc.WithBlock())

	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tracerProvider.Shutdown, nil
}

func main() {
	logger := logrus.StandardLogger()

	slurmConfig, err := slurm.NewSlurmConfig()
	if err != nil {
		panic(err)
	}

	if slurmConfig.VerboseLogging {
		logger.SetLevel(logrus.DebugLevel)
	} else if slurmConfig.ErrorsOnlyLogging {
		logger.SetLevel(logrus.ErrorLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	log.L = logruslogger.FromLogrus(logrus.NewEntry(logger))

	JobIDs := make(map[string]*slurm.JidStruct)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if os.Getenv("ENABLE_TRACING") == "1" {
		shutdown, err := initProvider(ctx)
		if err != nil {
			log.G(ctx).Fatal(err)
		}
		defer func() {
			if err = shutdown(ctx); err != nil {
				log.G(ctx).Fatal("failed to shutdown TracerProvider: %w", err)
			}
		}()

		log.G(ctx).Info("Tracer setup succeeded")

		// TODO: disable this through options
		trace.T = opentelemetry.Adapter{}
	}

	log.G(ctx).Debug("Debug level: " + strconv.FormatBool(slurmConfig.VerboseLogging))

	SidecarAPIs := slurm.SidecarHandler{
		Config: slurmConfig,
		JIDs:   &JobIDs,
		Ctx:    ctx,
	}

	mutex := http.NewServeMux()
	mutex.HandleFunc("/status", SidecarAPIs.StatusHandler)
	mutex.HandleFunc("/create", SidecarAPIs.SubmitHandler)
	mutex.HandleFunc("/delete", SidecarAPIs.StopHandler)
	mutex.HandleFunc("/getLogs", SidecarAPIs.GetLogsHandler)

	SidecarAPIs.CreateDirectories()
	SidecarAPIs.LoadJIDs()

	if strings.HasPrefix(slurmConfig.Socket, "unix://") {
		// Create a Unix domain socket and listen for incoming connections.
		socket, err := net.Listen("unix", strings.ReplaceAll(slurmConfig.Socket, "unix://", ""))
		if err != nil {
			panic(err)
		}

		// Cleanup the sockfile.
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			os.Remove(strings.ReplaceAll(slurmConfig.Socket, "unix://", ""))
			os.Exit(1)
		}()
		server := http.Server{
			Handler: mutex,
		}

		log.G(ctx).Info(socket)

		if err := server.Serve(socket); err != nil {
			log.G(ctx).Fatal(err)
		}
	} else {
		err = http.ListenAndServe(":"+slurmConfig.Sidecarport, mutex)
		if err != nil {
			log.G(ctx).Fatal(err)
		}
	}
}
