package slurm

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
)

// StopHandler runs a scancel command, updating JIDs and cached statuses
func (h *SidecarHandler) StopHandler(w http.ResponseWriter, r *http.Request) {
	log.G(h.Ctx).Info("Slurm Sidecar: received Stop call")
	statusCode := http.StatusOK

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(w, statusCode, err)
		return
	}

	var pod *v1.Pod
	err = json.Unmarshal(bodyBytes, &pod)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(w, statusCode, err)
		return
	}

	filesPath := h.Config.DataRootFolder + pod.Namespace + "-" + string(pod.UID)

	err = deleteContainer(h.Ctx, h.Config, string(pod.UID), h.JIDs, filesPath)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(w, statusCode, err)
		return
	}
	if os.Getenv("SHARED_FS") != "true" {
		err = os.RemoveAll(filesPath)
		if err != nil {
			statusCode = http.StatusInternalServerError
			h.handleError(w, statusCode, err)
			return
		}
	}

	w.WriteHeader(statusCode)
	if statusCode != http.StatusOK {
		w.Write([]byte("Some errors occurred deleting containers. Check Slurm Sidecar's logs"))
	} else {

		w.Write([]byte("All containers for submitted Pods have been deleted"))
	}
}
