package main

import (
	"context"
	"net/http"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	logruslogger "github.com/virtual-kubelet/virtual-kubelet/log/logrus"

	slurm "github.com/intertwin-eu/interlink-slurm-plugin/pkg/slurm"
)

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
	Ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.G(Ctx).Debug("Debug level: " + strconv.FormatBool(slurmConfig.VerboseLogging))

	SidecarAPIs := slurm.SidecarHandler{
		Config: slurmConfig,
		JIDs:   &JobIDs,
		Ctx:    Ctx,
	}

	mutex := http.NewServeMux()
	mutex.HandleFunc("/status", SidecarAPIs.StatusHandler)
	mutex.HandleFunc("/create", SidecarAPIs.SubmitHandler)
	mutex.HandleFunc("/delete", SidecarAPIs.StopHandler)
	mutex.HandleFunc("/getLogs", SidecarAPIs.GetLogsHandler)

	slurm.CreateDirectories(slurmConfig)
	slurm.LoadJIDs(Ctx, slurmConfig, &JobIDs)

	err = http.ListenAndServe(":"+slurmConfig.Sidecarport, mutex)
	if err != nil {
		log.G(Ctx).Fatal(err)
	}
}
