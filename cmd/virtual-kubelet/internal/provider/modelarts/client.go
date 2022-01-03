package modelarts

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"time"
)

const (
	QueryModelIntervalSecond = 60 * time.Second
	ModelPublishedStatus     = "published"

	SpecificationCPU = "modelarts.vm.cpu.2u"
	SpecificationGPU = "modelarts.vm.gpu.p4"

	RunningStatus   = "Running"
	FailedStatus    = "Failed"
	CompletedStatus = "Completed"
	PendingStatus   = "Pending"
)

type ModelartsClient struct {
	Client    *resty.Client
	ProjectID string
	Endpoint  string
}

type Model struct {
	Name        string `json:"model_name,omitempty"`
	ID          string `json:"model_id,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Version     string `json:"model_version,omitempty"`
	Image       string `json:"source_location,omitempty"`
	Type        string `json:"model_type,omitempty"`
	Status      string `json:"model_status,omitempty"`
	Desc        string `json:"description,omitempty"`
}

type Service struct {
	Name        string           `json:"service_name,omitempty"`
	ID          string           `json:"service_id,omitempty"`
	Type        string           `json:"infer_type,omitempty"`
	Status      string           `json:"status,omitempty"`
	Desc        string           `json:"description,omitempty"`
	WorkspaceID string           `json:"workspace_id,omitempty"`
	Config      []*ServiceConfig `json:"config,omitempty"`
	ResourceIDs []string         `json:"resource_ids,omitempty"`
}

type ServiceConfig struct {
	ModelID       string            `json:"model_id,omitempty"`
	Specification string            `json:"specification,omitempty"`
	InstanceCount int               `json:"instance_count,omitempty"`
	Envs          map[string]string `json:"envs,omitempty"`
}

type TrainJob struct {
	Name        string          `json:"job_name,omitempty"`
	ID          int64           `json:"job_id,omitempty"`
	VersionName string          `json:"version_name,omitempty"`
	VersionID   int64           `json:"version_id,omitempty"`
	Status      int64           `json:"status,omitempty"`
	Desc        string          `json:"job_desc,omitempty"`
	IsSuccess   bool            `json:"is_success,omitempty"`
	CreateTime  int64           `json:"create_time,omitempty"`
	WorkspaceID string          `json:"workspace_id,omitempty"`
	Config      *TrainJobConfig `json:"config,omitempty"`
}

type TrainJobConfig struct {
	WorkerServerNum int                 `json:"worker_server_num,omitempty"`
	Parameter       []map[string]string `json:"parameter,omitempty"`
	DataURL         string              `json:"data_url,omitempty"`
	UserImageURL    string              `json:"user_image_url,omitempty"`
	UserCommand     string              `json:"user_command,omitempty"`
	SpecID          int64               `json:"spec_id,omitempty"`
	PreVersionID    int64               `json:"pre_version_id,omitempty"`
}

func CreateModelartsClient(accessKeyID string, secretAccessKey string, region string) (*ModelartsClient, error) {
	mc := ModelartsClient{
		Client:   resty.New(),
		Endpoint: fmt.Sprintf("https://modelarts.%s.myhuaweicloud.com", region),
	}

	err := mc.initModelartsClient(accessKeyID, secretAccessKey, region)

	return &mc, err
}

func (mc *ModelartsClient) initModelartsClient(accessKeyID string, secretAccessKey string, region string) error {
	iamEndpoint := fmt.Sprintf("https://iam.%s.myhuaweicloud.com", region)
	url := fmt.Sprintf("%s/v3/auth/tokens", iamEndpoint)
	requestBody := fmt.Sprintf(`{"auth": {"identity": {"methods": ["hw_ak_sk"],"hw_ak_sk": {"access": {"key": "%s"}, "secret": {"key": "%s"}}},
                                                 "scope": {"project": {"name": "%s"}}}}`, accessKeyID, secretAccessKey, region)
	c := mc.Client
	resp, err := c.R().SetHeader("Content-Type", "application/json").SetBody(requestBody).Post(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return fmt.Errorf("initial modelarts client failed, error: %v", err)
	}

	mc.Client.SetAuthToken(resp.Header().Get("X-Subject-Token"))

	responseBody := struct {
		Token map[string]interface{} `json:"token"`
	}{}

	if err := json.Unmarshal(resp.Body(), &responseBody); err != nil {
		return err
	}

	mc.ProjectID = responseBody.Token["project"].(map[string]interface{})["id"].(string)

	return nil
}

func convertEnvs2Parameter(envs map[string]string) (parameter []map[string]string) {
	for k, v := range envs {
		parameter = append(parameter, map[string]string{
			"label": k,
			"value": v,
		})
	}
	return parameter
}

func (mc *ModelartsClient) CreateTrainJob(name string, dataURL string, image string, command string, envs map[string]string, specID int64, workerID string) (*TrainJob, error) {
	parameter := convertEnvs2Parameter(envs)

	config := &TrainJobConfig{
		WorkerServerNum: 1,
		Parameter:       parameter,
		DataURL:         dataURL,
		UserCommand:     command,
		UserImageURL:    image,
		SpecID:          specID,
	}

	job := TrainJob{
		Name:        name,
		WorkspaceID: workerID,
		Desc:        name,
		Config:      config,
	}

	url := fmt.Sprintf("%s/v1/%s/training-jobs", mc.Endpoint, mc.ProjectID)
	resp, err := mc.Client.R().SetBody(job).Post(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("create modelarts train job(name=%s) failed, error: %v", name, err)
	}

	if err := json.Unmarshal(resp.Body(), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (mc *ModelartsClient) UpdateTrainJobVersion(job *TrainJob, envs map[string]string) error {
	if envs == nil {
		job.Config.Parameter = convertEnvs2Parameter(envs)
	}

	url := fmt.Sprintf("%s/v1/%s/training-jobs/%d/versions", mc.Endpoint, mc.ProjectID, job.ID)
	if job.Config == nil {
		job.Config = &TrainJobConfig{}
	}
	job.Config.PreVersionID = job.VersionID
	resp, err := mc.Client.R().SetBody(job).Post(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return fmt.Errorf("update modelarts train job(name=%s) failed, error: %v", job.Name, err)
	}

	if err := json.Unmarshal(resp.Body(), job); err != nil {
		return err
	}

	return nil
}

func (mc *ModelartsClient) GetTrainJob(name string, workspaceID string) (trainJob *TrainJob, err error) {
	url := fmt.Sprintf("%s/v1/%s/training-jobs?search_content=%s&workspace_id=%s", mc.Endpoint, mc.ProjectID, name, workspaceID)
	resp, err := mc.Client.R().Get(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("query modelarts service(name=%s, wokspace_id=%s) failed, error: %v", name, workspaceID, err)
	}

	body := map[string]interface{}{}
	if err = json.Unmarshal(resp.Body(), &body); err != nil {
		return nil, err
	}

	if body["job_total_count"].(float64) == 0 {
		return nil, nil
	}

	jobsBytes, err := json.Marshal(body["jobs"])
	if err != nil {
		return nil, err
	}

	var jobs []map[string]interface{}
	if err = json.Unmarshal(jobsBytes, &jobs); err != nil {
		return nil, err
	}

	for _, job := range jobs {
		desc := job["job_desc"]
		if desc == name {
			trainJob = &TrainJob{Name: name}
			trainJob.ID = int64(job["job_id"].(float64))
			trainJob.Desc = job["job_desc"].(string)
			trainJob.VersionID = int64(job["version_id"].(float64))
			trainJob.Status = int64(job["status"].(float64))
			break
		}
	}

	return
}

func (mc *ModelartsClient) ConvertTrainStatus(v interface{}) string {
	switch v.(int64) {
	case 1, 2, 4, 8:
		return RunningStatus
	case 7:
		return PendingStatus
	case 10:
		return CompletedStatus
	default:
		return FailedStatus
	}
}

func (mc *ModelartsClient) DeleteTrainJob(name string, workspaceID string) error {
	var trainJob *TrainJob
	var err error

	if trainJob, err = mc.GetTrainJob(name, workspaceID); err != nil {
		return err
	}

	if trainJob == nil {
		return nil
	}

	url := fmt.Sprintf("%s/v1/%s/training-jobs/%d", mc.Endpoint, mc.ProjectID, trainJob.ID)
	resp, err := mc.Client.R().Delete(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return fmt.Errorf("delete modelarts train job(name=%s) failed, error: %v", name, err)
	}

	return nil
}

func (mc *ModelartsClient) CreateModel(name string, image string, workspaceID string) (*Model, error) {
	url := fmt.Sprintf("%s/v1/%s/models", mc.Endpoint, mc.ProjectID)
	model := Model{
		Name:        name,
		WorkspaceID: workspaceID,
		Version:     "1.0.0",
		Image:       image,
		Type:        "Image",
		Desc:        name,
	}
	resp, err := mc.Client.R().SetBody(model).Post(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("create modelarts model(name=%s) failed, error: %v", name, err)
	}

	if err := json.Unmarshal(resp.Body(), &model); err != nil {
		return nil, err
	}

	return &model, err
}

func (mc *ModelartsClient) GetModel(name string, workspaceID string) (model *Model, err error) {
	url := fmt.Sprintf("%s/v1/%s/models?model_name=%s&workspace_id=%s", mc.Endpoint, mc.ProjectID, name, workspaceID)
	resp, err := mc.Client.R().Get(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("query modelarts model(name=%s, wokspace_id=%s) failed, error: %v", name, workspaceID, err)
	}

	body := map[string]interface{}{}
	if err = json.Unmarshal(resp.Body(), &body); err != nil {
		return nil, err
	}

	if body["count"].(float64) == 0 {
		return nil, nil
	}

	modelsBytes, err := json.Marshal(body["models"])
	if err != nil {
		return nil, err
	}

	var models []map[string]interface{}
	if err = json.Unmarshal(modelsBytes, &models); err != nil {
		return nil, err
	}

	for _, m := range models {
		if m["description"] == name {
			model = &Model{Name: name}
			model.ID = m["model_id"].(string)
			model.Status = m["model_status"].(string)
			break
		}
	}

	return
}

func (mc *ModelartsClient) deleteModel(model *Model) error {
	url := fmt.Sprintf("%s/v1/%s/models/%d", mc.Endpoint, mc.ProjectID, model.ID)
	resp, err := mc.Client.R().Delete(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return fmt.Errorf("delete modelarts train job(name=%s) failed, error: %v", model.ID, err)
	}

	return nil
}

func (mc *ModelartsClient) GetModelStatus(model *Model) (string, error) {
	url := fmt.Sprintf("%s/v1/%s/models/%s", mc.Endpoint, mc.ProjectID, model.ID)
	resp, err := mc.Client.R().Get(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return "", fmt.Errorf("query modelarts model(name=%s, version=%s) failed, error: %v", model.Name, model.Version, err)
	}

	body := map[string]interface{}{}
	if err := json.Unmarshal(resp.Body(), &body); err != nil {
		return "", err
	}

	return body["model_status"].(string), nil
}

func (mc *ModelartsClient) CreateService(name string, inferType string, model *Model,
	specification string, envs map[string]string, workerID string) (*Service, error) {
	for {
		if status, err := mc.GetModelStatus(model); err != nil {
			return nil, err
		} else if status == ModelPublishedStatus {
			break
		}

		time.Sleep(QueryModelIntervalSecond)
	}

	url := fmt.Sprintf("%s/v1/%s/services", mc.Endpoint, mc.ProjectID)

	getSpecification := func(specification string) string {
		if specification == "gpu" {
			return SpecificationGPU
		}
		return SpecificationCPU
	}

	config := &ServiceConfig{
		ModelID:       model.ID,
		Specification: getSpecification(specification),
		InstanceCount: 1,
		Envs:          envs,
	}

	service := Service{
		Name:        name,
		WorkspaceID: workerID,
		Type:        inferType,
		Desc:        name,
		Config:      []*ServiceConfig{config},
	}

	resp, err := mc.Client.R().SetBody(service).Post(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("create modelarts service(name=%s) failed, error: %v", name, err)
	}

	if err := json.Unmarshal(resp.Body(), &service); err != nil {
		return nil, err
	}

	return &service, err
}

func (mc *ModelartsClient) DeleteService(name string, workspaceID string) error {
	var err error
	var service *Service

	if service, err = mc.GetService(name, workspaceID); err != nil {
		return err
	}

	if service == nil {
		return nil
	}

	url := fmt.Sprintf("%s/v1/%s/services/%s", mc.Endpoint, mc.ProjectID, service.ID)
	resp, err := mc.Client.R().Delete(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return fmt.Errorf("delete modelarts service(name=%s) failed, error: %v", service.Name, err)
	}

	return nil
}

func (mc *ModelartsClient) GetService(name string, workspaceID string) (service *Service, err error) {
	url := fmt.Sprintf("%s/v1/%s/services?service_name=%s&workspace_id=%s", mc.Endpoint, mc.ProjectID, name, workspaceID)
	resp, err := mc.Client.R().Get(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("query modelarts service(name=%s, wokspace_id=%s) failed, error: %v", name, workspaceID, err)
	}

	body := map[string]interface{}{}
	if err = json.Unmarshal(resp.Body(), &body); err != nil {
		return nil, err
	}

	if body["count"].(float64) == 0 {
		return nil, nil
	}

	servicesBytes, err := json.Marshal(body["services"])
	if err != nil {
		return nil, err
	}

	var services []map[string]interface{}
	if err = json.Unmarshal(servicesBytes, &services); err != nil {
		return nil, err
	}

	for _, s := range services {
		if s["description"] == name {
			service = &Service{Name: name}
			service.ID = s["service_id"].(string)
			service.Status = s["status"].(string)
			break
		}
	}

	return
}

func (mc *ModelartsClient) UpdateService(service *Service, model *Model) (*Service, error) {
	for {
		if status, err := mc.GetModelStatus(model); err != nil {
			return nil, err
		} else if status == ModelPublishedStatus {
			break
		}

		time.Sleep(QueryModelIntervalSecond)
	}

	config := service.Config
	if config == nil {
		config = []*ServiceConfig{}
	}

	if len(config) == 1 {
		config[0].ModelID = model.ID
	}

	s := Service{}
	s.Config = config
	url := fmt.Sprintf("%s/v1/%s/services/%s", mc.Endpoint, mc.ProjectID, service.ID)

	resp, err := mc.Client.R().SetBody(s).Put(url)
	if err != nil || resp.IsError() {
		if resp.IsError() {
			err = fmt.Errorf(resp.String())
		}
		return nil, fmt.Errorf("update modelarts servic(name=%s) failed, error: %v", service.Name, err)
	}

	return service, err
}
