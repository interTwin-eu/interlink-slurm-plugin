package slurm

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"

	"github.com/containerd/containerd/log"

	commonIL "github.com/intertwin-eu/interlink/pkg/common"
)

// SubmitHandler generates and submits a SLURM batch script according to provided data.
// 1 Pod = 1 Job. If a Pod has multiple containers, every container is a line with it's parameters in the SLURM script.
func (h *SidecarHandler) SubmitHandler(w http.ResponseWriter, r *http.Request) {
	log.G(h.Ctx).Info("Slurm Sidecar: received Submit call")
	statusCode := http.StatusOK
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		w.WriteHeader(statusCode)
		w.Write([]byte("Some errors occurred while creating container. Check Slurm Sidecar's logs"))
		log.G(h.Ctx).Error(err)
		return
	}

	var req []commonIL.RetrievedPodData
	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		statusCode = http.StatusInternalServerError
		w.WriteHeader(statusCode)
		w.Write([]byte("Some errors occurred while creating container. Check Slurm Sidecar's logs"))
		log.G(h.Ctx).Error(err)
		return
	}

	for _, data := range req {
		containers := data.Pod.Spec.InitContainers
		containers = append(containers, data.Pod.Spec.Containers...)
		metadata := data.Pod.ObjectMeta
		filesPath := h.Config.DataRootFolder + data.Pod.Namespace + "-" + string(data.Pod.UID)

		var singularity_command_pod []SingularityCommand
		var resourceLimits ResourceLimits

		for _, container := range containers {
			log.G(h.Ctx).Info("- Beginning script generation for container " + container.Name)
			singularityPrefix := commonIL.InterLinkConfigInst.SingularityPrefix
			if singularityAnnotation, ok := metadata.Annotations["slurm-job.vk.io/singularity-commands"]; ok {
				singularityPrefix += " " + singularityAnnotation
			}

			singularityMounts := ""
			if singMounts, ok := metadata.Annotations["slurm-job.vk.io/singularity-mounts"]; ok {
				singularityMounts = singMounts
			}

			singularityOptions := ""
			if singOpts, ok := metadata.Annotations["slurm-job.vk.io/singularity-options"]; ok {
				singularityOptions = singOpts
			}

			commstr1 := []string{"singularity", "run", "--containall", "--nv", singularityMounts, singularityOptions}

			/*var commstr1 []string
			if len(container.Command) != 0 {
				commstr1 = []string{"singularity", "exec", "--containall", "--nv", singularityMounts, singularityOptions}

			} else {
				commstr1 = []string{"singularity", "exec", "--containall", "--nv", singularityMounts, singularityOptions}
			}*/

			envs := prepareEnvs(h.Ctx, container)
			image := ""

			CPULimit, _ := container.Resources.Limits.Cpu().AsInt64()
			MemoryLimit, _ := container.Resources.Limits.Memory().AsInt64()
			if CPULimit == 0 {
				log.G(h.Ctx).Warning(errors.New("Max CPU resource not set for " + container.Name + ". Only 1 CPU will be used"))
				resourceLimits.CPU += 1
			} else {
				resourceLimits.CPU += CPULimit
			}
			if MemoryLimit == 0 {
				log.G(h.Ctx).Warning(errors.New("Max Memory resource not set for " + container.Name + ". Only 1MB will be used"))
				resourceLimits.Memory += 1024 * 1024
			} else {
				resourceLimits.Memory += MemoryLimit
			}

			mounts, err := prepareMounts(h.Ctx, h.Config, req, container, filesPath)
			log.G(h.Ctx).Debug(mounts)
			if err != nil {
				statusCode = http.StatusInternalServerError
				w.WriteHeader(statusCode)
				w.Write([]byte("Error prepairing mounts. Check Slurm Sidecar's logs"))
				log.G(h.Ctx).Error(err)
				os.RemoveAll(filesPath)
				return
			}

			image = container.Image
			if image_uri, ok := metadata.Annotations["slurm-job.vk.io/image-root"]; ok {
				image = image_uri + container.Image
			} else {
				log.G(h.Ctx).Info("- image-uri annotation not specified for path in remote filesystem")
			}

			log.G(h.Ctx).Debug("-- Appending all commands together...")
			singularity_command := append(commstr1, envs...)
			singularity_command = append(singularity_command, mounts)
			singularity_command = append(singularity_command, image)

			singularity_command_pod = append(singularity_command_pod, SingularityCommand{singularityCommand: singularity_command, containerName: container.Name, containerArgs: container.Args, containerCommand: container.Command})
		}

		path, err := produceSLURMScript(h.Ctx, h.Config, string(data.Pod.UID), filesPath, metadata, singularity_command_pod, resourceLimits)
		if err != nil {
			statusCode = http.StatusInternalServerError
			w.WriteHeader(statusCode)
			w.Write([]byte("Error producing Slurm script. Check Slurm Sidecar's logs"))
			log.G(h.Ctx).Error(err)
			os.RemoveAll(filesPath)
			return
		}
		out, err := SLURMBatchSubmit(h.Ctx, h.Config, path)
		if err != nil {
			statusCode = http.StatusInternalServerError
			w.WriteHeader(statusCode)
			w.Write([]byte("Error submitting Slurm script. Check Slurm Sidecar's logs"))
			log.G(h.Ctx).Error(err)
			os.RemoveAll(filesPath)
			return
		}
		log.G(h.Ctx).Info(out)
		err = handleJidAndPodUid(h.Ctx, data.Pod, h.JIDs, out, filesPath)
		if err != nil {
			statusCode = http.StatusInternalServerError
			w.WriteHeader(statusCode)
			w.Write([]byte("Error handling JID. Check Slurm Sidecar's logs"))
			log.G(h.Ctx).Error(err)
			os.RemoveAll(filesPath)
			err = deleteContainer(h.Ctx, h.Config, string(data.Pod.UID), h.JIDs, filesPath)
			return
		}
	}

	w.WriteHeader(statusCode)

	if statusCode != http.StatusOK {
		w.Write([]byte("Some errors occurred while creating containers. Check Slurm Sidecar's logs"))
	} else {
		w.Write([]byte("Containers created"))
	}
}
