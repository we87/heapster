package v1

import (
	"time"

	"strings"

	"k8s.io/heapster/sinks/cache"
	"k8s.io/heapster/util"
)

type rancherDecoder struct {
	*decoder
}

func (self *rancherDecoder) TimeseriesFromContainers(containers []*cache.ContainerElement) ([]Timeseries, error) {
	labels := make(map[string]string)
	var result []Timeseries
	for index := range containers {
		if self.isRancherContainer(containers[index]) {
			result = append(result, self.getContainerMetrics(self.mapToK8sContainer(containers[index]))...)
			continue
		}
		labels[LabelHostname.Key] = containers[index].Hostname
		result = append(result, self.getContainerMetrics(containers[index], util.CopyLabels(labels))...)
	}
	return result, nil
}

func (self *rancherDecoder) isRancherContainer(container *cache.ContainerElement) bool {
	return len(container.Labels) > 0 && container.Labels["io.rancher.project.name"] != "" && container.Labels["io.rancher.project_service.name"] != ""
}

func (self *rancherDecoder) mapToK8sContainer(container *cache.ContainerElement) (*cache.ContainerElement, map[string]string) {
	cLabel := container.Labels
	labels := make(map[string]string)

	// hostname
	labels[LabelHostname.Key] = container.Hostname

	// map namespace to project
	labels[LabelPodNamespace.Key] = cLabel["io.rancher.project.name"]

	// map pod and container name
	ssName := cLabel["io.rancher.project_service.name"]
	containerName := ssName
	com := strings.SplitN(containerName, "/", 2)
	if len(com) == 2 {
		labels[LabelPodName.Key] = com[0]
		containerName = com[1]
		if h, ok := cLabel["io.rancher.container.uuid"]; ok {
			containerName += "-" + h[:8]
		}
	}
	// override container's name
	container.Name = containerName

	// TODO(antmanler): figure out how to map, or can be safely ignored?
	//labels[LabelPodNamespaceUID.Ky] = pod.NamespaceUID
	//labels[LabelPodId.Key] = pod.PodId

	return container, labels
}

func NewDecoderWithRancher() Decoder {
	// Get supported metrics.
	return &rancherDecoder{
		decoder: &decoder{
			supportedStatMetrics: statMetrics,
			lastExported:         make(map[timeseriesKey]time.Time),
		},
	}
}
