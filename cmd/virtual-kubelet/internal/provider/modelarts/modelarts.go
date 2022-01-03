package modelarts

import (
	"context"
	"fmt"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	stats "github.com/virtual-kubelet/virtual-kubelet/node/api/statsv1alpha1"
	"github.com/virtual-kubelet/virtual-kubelet/trace"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Values used in tracing as attribute keys.
	namespaceKey = "namespace"
	nameKey      = "name"

	// TrainPodType is type of train pod
	TrainPodType = "train"
	// EvalPodType is type of eval pod
	EvalPodType = "eval"
	// InferencePodType is type of inference pod
	InferencePodType = "inference"
)

// ModelartsProvider implements the virtual-kubelet provider interface and stores pods in memory.
type Provider struct {
	client             *ModelartsClient
	workerSpaceID      string
	nodeName           string
	operatingSystem    string
	internalIP         string
	daemonEndpointPort int32
	pods               map[string]*v1.Pod
	startTime          time.Time
}

// GetPods returns a list of all pods known to be "running".
func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	ctx, span := trace.StartSpan(ctx, "GetPods")
	defer span.End()

	log.G(ctx).Info("receive GetPods")

	var pods []*v1.Pod

	for _, pod := range p.pods {
		pods = append(pods, pod)
	}

	return pods, nil
}

func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader("")), nil
}

func (p *Provider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
	return nil
}

// NewModelartsProvider creates a new ModelartsProvider, which implements the PodNotifier interface
func NewModelartsProvider(providerConfig, nodeName, operatingSystem string, internalIP string, daemonEndpointPort int32) (*Provider, error) {
	ak := os.Getenv("ACCESS_KEY_ID")
	sk := os.Getenv("SECRET_ACCESS_KEY")
	region := os.Getenv("REGION")
	workSpaceID := os.Getenv("WORKSPACEID")
	if workSpaceID == "" {
		workSpaceID = "0"
	}

	client, err := CreateModelartsClient(ak, sk, region)
	if err != nil {
		return nil, err
	}

	provider := Provider{
		client:             client,
		workerSpaceID:      workSpaceID,
		nodeName:           nodeName,
		operatingSystem:    operatingSystem,
		internalIP:         internalIP,
		daemonEndpointPort: daemonEndpointPort,
		pods:               make(map[string]*v1.Pod),
		startTime:          time.Now(),
	}

	return &provider, nil
}

func (p *Provider) addPodAnnotations(pod *v1.Pod, key string, value interface{}) {
	ann := pod.GetAnnotations()
	ann[key] = value.(string)
	pod.SetAnnotations(ann)
}

func (p *Provider) createTask(ctx context.Context, pod *v1.Pod, taskType string) error {
	var err error
	var jobName string

	if jobName, err = buildJobName(pod); err != nil {
		return err
	}

	taskName := buildTaskName(jobName, taskType)

	log.G(ctx).Infof("job(name=%s) creates modelarts train job(name=%s) in %s phase", jobName, taskName, taskType)

	container := pod.Spec.Containers[0]

	image := container.Image

	commands := container.Command
	commands = append(commands, container.Args...)
	commandStr := strings.Join(commands, " ")

	envs := map[string]string{}
	for _, env := range container.Env {
		envs[env.Name] = env.Value
	}

	ann := pod.GetAnnotations()
	data, ok := ann["data"]
	if !ok {
		return fmt.Errorf("job(name=%s)'s pod(name=%s) has not found data in annotations in %s phase", jobName, pod.Name, taskType)
	}
	u, _ := url.Parse(data)
	dataURL := filepath.Join("/", u.Host, u.Path)

	var trainJob *TrainJob
	if trainJob, err = p.client.GetTrainJob(taskName, p.workerSpaceID); err != nil {
		return err
	}

	if trainJob == nil {
		if trainJob, err = p.client.CreateTrainJob(taskName, dataURL, image, commandStr, envs, 1, p.workerSpaceID); err != nil {
			return err
		}
	} else {
		trainJob.Config = &TrainJobConfig{
			WorkerServerNum: 1,
			DataURL:         dataURL,
			UserCommand:     commandStr,
			UserImageURL:    image,
			SpecID:          1,
		}
		if err = p.client.UpdateTrainJobVersion(trainJob, envs); err != nil {
			return err
		}
	}

	log.G(ctx).Infof("job(name=%s) creates modelarts train job(name=%s, version=%s) in %s phase successfully !",
		jobName, taskName, trainJob.VersionName, taskType)

	return nil
}

func (p *Provider) createService(ctx context.Context, pod *v1.Pod) error {
	var err error
	var jobName string

	if jobName, err = buildJobName(pod); err != nil {
		return err
	}

	var model *Model
	model, err = p.client.GetModel(jobName, p.workerSpaceID)
	if err != nil {
		return err
	}

	container := pod.Spec.Containers[0]
	if model == nil {
		image := container.Image
		model, err = p.client.CreateModel(jobName, image, p.workerSpaceID)
		if err != nil {
			return err
		}
	}

	envs := map[string]string{}
	for _, env := range container.Env {
		envs[env.Name] = env.Value
	}

	var service *Service
	service, err = p.client.GetService(jobName, p.workerSpaceID)
	if err != nil {
		return err
	}

	if service == nil {
		log.G(ctx).Infof("job(name=%s) create modelarts service", jobName)
		// env SERVICE_SPECIFICATION has two values: "cpu", "gpu"
		spec := os.Getenv("SERVICE_SPECIFICATION")
		if spec != "gpu" {
			spec = "cpu"
		}

		if service, err = p.client.CreateService(jobName, "real-time", model, spec, envs, p.workerSpaceID); err != nil {
			log.G(ctx).Errorf("job(name=%s) create modelarts service failed, error: %v", jobName, err)
		}
	} else {
		log.G(ctx).Infof("job(name=%s) update modelarts service", jobName)
		if service, err = p.client.UpdateService(service, model); err != nil {
			log.G(ctx).Errorf("job(name=%s) update modelarts service failed, error: %v", jobName, err)
		}
	}

	log.G(ctx).Infof("job(name=%s) create modelarts service successfully!", jobName)

	return nil
}

func (p *Provider) setCreatePodStatus(pod *v1.Pod) {
	now := metav1.NewTime(time.Now())
	pod.Status = v1.PodStatus{
		Phase:     v1.PodRunning,
		StartTime: &now,
		Conditions: []v1.PodCondition{
			{
				Type:   v1.PodInitialized,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodReady,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
		},
	}

	for _, container := range pod.Spec.Containers {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, v1.ContainerStatus{
			Name:         container.Name,
			Image:        container.Image,
			Ready:        true,
			RestartCount: 0,
			State: v1.ContainerState{
				Running: &v1.ContainerStateRunning{
					StartedAt: now,
				},
			},
		})
	}
}

func (p *Provider) setInitPodStatus(pod *v1.Pod) {
	now := metav1.NewTime(time.Now())
	pod.Status = v1.PodStatus{
		Phase:     v1.PodPending,
		StartTime: &now,
		Conditions: []v1.PodCondition{
			{
				Type:   v1.PodInitialized,
				Status: v1.ConditionTrue,
			},
		},
	}
}

// CreatePod accepts a Pod definition and stores it in memory.
func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {

	ctx, span := trace.StartSpan(ctx, "CreatePod")
	defer span.End()

	// Add the pod's coordinates to the current span.
	ctx = addAttributes(ctx, span, namespaceKey, pod.Namespace, nameKey, pod.Name)

	log.G(ctx).Infof("receive CreatePod %q", pod.Name)

	var err error
	var key string
	key, err = buildKey(pod)
	if err != nil {
		return err
	}

	if _, ok := p.pods[key]; ok {
		log.G(ctx).Errorf("pod(%q) has created", pod.Name)
		return nil
	}

	p.setInitPodStatus(pod)
	p.pods[key] = pod

	ann := pod.GetAnnotations()
	podType, ok := ann["type"]
	if !ok {
		return fmt.Errorf("not found pod type of annotations")
	}

	switch podType {
	case TrainPodType, EvalPodType:
		err = p.createTask(ctx, pod, podType)
	case InferencePodType:
		err = p.createService(ctx, pod)
	default:
		return fmt.Errorf("unvilid pod type: %s", podType)
	}
	if err != nil {
		return err
	}

	p.setCreatePodStatus(pod)

	p.pods[key] = pod

	return nil
}

// DeletePod deletes the specified pod out of memory.
func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) (err error) {
	ctx, span := trace.StartSpan(ctx, "DeletePod")
	defer span.End()

	// Add the pod's coordinates to the current span.
	ctx = addAttributes(ctx, span, namespaceKey, pod.Namespace, nameKey, pod.Name)

	log.G(ctx).Infof("receive DeletePod %q", pod.Name)

	var key string
	key, err = buildKey(pod)
	if err != nil {
		return err
	}

	if _, exists := p.pods[key]; !exists {
		return errdefs.NotFound("pod not found")
	}

	var jobName string
	if jobName, err = buildJobName(pod); err != nil {
		return err
	}

	ann := pod.GetAnnotations()
	podType := ann["type"]
	taskName := buildTaskName(jobName, podType)
	switch podType {
	case TrainPodType, EvalPodType:
		err = p.client.DeleteTrainJob(taskName, p.workerSpaceID)
	case InferencePodType:
		err = p.client.DeleteService(jobName, p.workerSpaceID)
	default:
		return fmt.Errorf("unvilid pod type: %s", podType)
	}

	if err != nil {
		return fmt.Errorf("delete task(name=%s) failed, error: %v", key, err)
	}

	delete(p.pods, key)

	return nil
}

// GetPodStatus returns the status of a pod by name that is "running".
// returns nil if a pod by that name is not found.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	ctx, span := trace.StartSpan(ctx, "GetPodStatus")
	defer span.End()

	// Add namespace and name as attributes to the current span.
	ctx = addAttributes(ctx, span, namespaceKey, namespace, nameKey, name)

	log.G(ctx).Infof("receive GetPodStatus %q", name)

	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	var status string

	ann := pod.GetAnnotations()
	podType := ann["type"]

	var jobName string
	if jobName, err = buildJobName(pod); err != nil {
		return nil, err
	}
	taskName := buildTaskName(jobName, podType)

	switch podType {
	case TrainPodType, EvalPodType:
		var trainJob *TrainJob
		if trainJob, err = p.client.GetTrainJob(taskName, p.workerSpaceID); err != nil {
			return nil, err
		}

		if trainJob == nil {
			status = PendingStatus
		} else {
			status = p.client.ConvertTrainStatus(trainJob.Status)
		}
	case InferencePodType:
		var service *Service
		if service, err = p.client.GetService(jobName, p.workerSpaceID); err != nil {
			return nil, err
		}

		if service == nil {
			status = PendingStatus
		} else {
			status = service.Status
		}

	default:
		return nil, fmt.Errorf("unvilid pod type: %s", podType)
	}
	if err != nil {
		return nil, err
	}

	switch status {
	case RunningStatus:
		pod.Status.Phase = v1.PodRunning
	case FailedStatus:
		pod.Status.Phase = v1.PodFailed
		p.closeContainers(pod, status)
	case CompletedStatus:
		pod.Status.Phase = v1.PodSucceeded
		p.closeContainers(pod, status)
	case PendingStatus:
		pod.Status.Phase = v1.PodPending
	}

	return &pod.Status, nil
}

func (p *Provider) closeContainers(pod *v1.Pod, podStatus string) {
	now := metav1.Now()
	for idx := range pod.Status.ContainerStatuses {
		pod.Status.ContainerStatuses[idx].Ready = false
		pod.Status.ContainerStatuses[idx].State = v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{
				FinishedAt: now,
				Reason:     podStatus,
				StartedAt:  pod.Status.ContainerStatuses[idx].State.Running.StartedAt,
			},
		}
	}
}

// GetPod returns a pod by name that is stored in memory.
func (p *Provider) GetPod(ctx context.Context, namespace, name string) (pod *v1.Pod, err error) {
	ctx, span := trace.StartSpan(ctx, "GetPod")
	defer func() {
		span.SetStatus(err)
		span.End()
	}()

	// Add the pod's coordinates to the current span.
	ctx = addAttributes(ctx, span, namespaceKey, namespace, nameKey, name)

	log.G(ctx).Infof("receive GetPod %q", name)
	key, err := buildKeyFromNames(namespace, name)
	if err != nil {
		return nil, err
	}

	if pod, ok := p.pods[key]; ok {
		return pod, nil
	}
	return nil, errdefs.NotFoundf("pod %s/%s is not known to the provider", namespace, name)
}

func (p *Provider) ConfigureNode(ctx context.Context, n *v1.Node) { // nolint:golint
	ctx, span := trace.StartSpan(ctx, "modelarts.ConfigureNode") // nolint:staticcheck,ineffassign
	defer span.End()

	n.Status.Conditions = p.nodeConditions()
	n.Status.Addresses = p.nodeAddresses()
	n.Status.DaemonEndpoints = p.nodeDaemonEndpoints()
	os := p.operatingSystem
	if os == "" {
		os = "linux"
	}
	n.Status.NodeInfo.OperatingSystem = os
	n.Status.NodeInfo.Architecture = "amd64"
	n.ObjectMeta.Labels["alpha.service-controller.kubernetes.io/exclude-balancer"] = "true"
	n.ObjectMeta.Labels["node.kubernetes.io/exclude-from-external-load-balancers"] = "true"
}

// UpdatePod accepts a Pod definition and updates its reference.
func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "UpdatePod")
	defer span.End()

	// Add the pod's coordinates to the current span.
	ctx = addAttributes(ctx, span, namespaceKey, pod.Namespace, nameKey, pod.Name)

	log.G(ctx).Infof("receive UpdatePod %q", pod.Name)

	key, err := buildKey(pod)
	if err != nil {
		return err
	}

	p.pods[key] = pod

	return nil
}

// NodeConditions returns a list of conditions (Ready, OutOfDisk, etc), for updates to the node status
// within Kubernetes.
func (p *Provider) nodeConditions() []v1.NodeCondition {
	// TODO: Make this configurable
	return []v1.NodeCondition{
		{
			Type:               "Ready",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletPending",
			Message:            "kubelet is pending.",
		},
		{
			Type:               "OutOfDisk",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientDisk",
			Message:            "kubelet has sufficient disk space available",
		},
		{
			Type:               "MemoryPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
		{
			Type:               "NetworkUnavailable",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "RouteCreated",
			Message:            "RouteController created a route",
		},
	}

}

// NodeAddresses returns a list of addresses for the node status
// within Kubernetes.
func (p *Provider) nodeAddresses() []v1.NodeAddress {
	return []v1.NodeAddress{
		{
			Type:    "InternalIP",
			Address: p.internalIP,
		},
	}
}

// NodeDaemonEndpoints returns NodeDaemonEndpoints for the node status
// within Kubernetes.
func (p *Provider) nodeDaemonEndpoints() v1.NodeDaemonEndpoints {
	return v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.daemonEndpointPort,
		},
	}
}

// GetStatsSummary returns dummy stats for all pods known by this provider.
func (p *Provider) GetStatsSummary(ctx context.Context) (*stats.Summary, error) {
	var span trace.Span
	ctx, span = trace.StartSpan(ctx, "GetStatsSummary") //nolint: ineffassign,staticcheck
	defer span.End()

	// Grab the current timestamp so we can report it as the time the stats were generated.
	time := metav1.NewTime(time.Now())

	// Create the Summary object that will later be populated with node and pod stats.
	res := &stats.Summary{}

	// Populate the Summary object with basic node stats.
	res.Node = stats.NodeStats{
		NodeName:  p.nodeName,
		StartTime: metav1.NewTime(p.startTime),
	}

	// Populate the Summary object with dummy stats for each pod known by this provider.
	for _, pod := range p.pods {
		var (
			// totalUsageNanoCores will be populated with the sum of the values of UsageNanoCores computes across all containers in the pod.
			totalUsageNanoCores uint64
			// totalUsageBytes will be populated with the sum of the values of UsageBytes computed across all containers in the pod.
			totalUsageBytes uint64
		)

		// Create a PodStats object to populate with pod stats.
		pss := stats.PodStats{
			PodRef: stats.PodReference{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				UID:       string(pod.UID),
			},
			StartTime: pod.CreationTimestamp,
		}

		// Iterate over all containers in the current pod to compute dummy stats.
		for _, container := range pod.Spec.Containers {
			// Grab a dummy value to be used as the total CPU usage.
			// The value should fit a uint32 in order to avoid overflows later on when computing pod stats.
			dummyUsageNanoCores := uint64(rand.Uint32())
			totalUsageNanoCores += dummyUsageNanoCores
			// Create a dummy value to be used as the total RAM usage.
			// The value should fit a uint32 in order to avoid overflows later on when computing pod stats.
			dummyUsageBytes := uint64(rand.Uint32())
			totalUsageBytes += dummyUsageBytes
			// Append a ContainerStats object containing the dummy stats to the PodStats object.
			pss.Containers = append(pss.Containers, stats.ContainerStats{
				Name:      container.Name,
				StartTime: pod.CreationTimestamp,
				CPU: &stats.CPUStats{
					Time:           time,
					UsageNanoCores: &dummyUsageNanoCores,
				},
				Memory: &stats.MemoryStats{
					Time:       time,
					UsageBytes: &dummyUsageBytes,
				},
			})
		}

		// Populate the CPU and RAM stats for the pod and append the PodsStats object to the Summary object to be returned.
		pss.CPU = &stats.CPUStats{
			Time:           time,
			UsageNanoCores: &totalUsageNanoCores,
		}
		pss.Memory = &stats.MemoryStats{
			Time:       time,
			UsageBytes: &totalUsageBytes,
		}
		res.Pods = append(res.Pods, pss)
	}

	// Return the dummy stats.
	return res, nil
}

func buildJobName(pod *v1.Pod) (string, error) {
	if err := checkPod(pod); err != nil {
		return "", err
	}

	// must have one owner
	ownerJob := pod.OwnerReferences[0]

	return buildKeyFromNames(pod.ObjectMeta.Namespace, ownerJob.Name)
}

func buildTaskName(jobName string, taskType string) string {
	return fmt.Sprintf("%s-%s", jobName, taskType)
}

func buildKeyFromNames(namespace string, name string) (string, error) {
	return fmt.Sprintf("%s-%s", namespace, name), nil
}

func checkPod(pod *v1.Pod) (err error) {
	if pod.ObjectMeta.Namespace == "" {
		return fmt.Errorf("pod namespace not found")
	}

	if pod.ObjectMeta.Name == "" {
		return fmt.Errorf("pod name not found")
	}

	return
}

// buildKey is a helper for building the "key" for the providers pod store.
func buildKey(pod *v1.Pod) (string, error) {
	if err := checkPod(pod); err != nil {
		return "", err
	}

	return buildKeyFromNames(pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
}

// addAttributes adds the specified attributes to the provided span.
// attrs must be an even-sized list of string arguments.
// Otherwise, the span won't be modified.
// TODO: Refactor and move to a "tracing utilities" package.
func addAttributes(ctx context.Context, span trace.Span, attrs ...string) context.Context {
	if len(attrs)%2 == 1 {
		return ctx
	}
	for i := 0; i < len(attrs); i += 2 {
		ctx = span.WithField(ctx, attrs[i], attrs[i+1])
	}
	return ctx
}
