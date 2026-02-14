export enum JobStatus {
  DEPLOYING = "DEPLOYING",
  RUNNING = "RUNNING",
  FAILED = "FAILED",
  SUSPENDED = "SUSPENDED",
  UPGRADING = "UPGRADING",
  UNKNOWN = "UNKNOWN",
}

export interface CreateJobRequest {
  jobName: string;
  pipelineYaml: string;
  flinkImage: string;
  resources?: ResourceSpec;
  parallelism?: number;
  flink?: FlinkSpec;
  namespace?: string;
}

export interface ResourceSpec {
  jobManager?: ManagerResource;
  taskManager?: TaskManagerResource;
}

export interface ManagerResource {
  cpu?: number;
  memory?: string;
}

export interface TaskManagerResource {
  cpu?: number;
  memory?: string;
  replicas?: number;
}

export interface FlinkSpec {
  version?: string;
  serviceAccount?: string;
  extraConfig?: Record<string, string>;
}

export interface JobSummary {
  jobName: string;
  namespace: string;
  status: JobStatus;
  flinkImage: string | null;
  createdAt: string | null;
  parallelism: number | null;
}

export interface JobDetail {
  jobName: string;
  namespace: string;
  status: JobStatus;
  flinkImage: string | null;
  createdAt: string | null;
  pipelineYaml: string | null;
  resources: ResourceInfo | null;
  parallelism: number | null;
  kubernetes: KubernetesStatus | null;
  flinkUiUrl: string | null;
  events: EventInfo[] | null;
}

export interface ResourceInfo {
  jobManager: ManagerResourceInfo | null;
  taskManager: TaskManagerResourceInfo | null;
}

export interface ManagerResourceInfo {
  cpu: number | string | null;
  memory: string | null;
}

export interface TaskManagerResourceInfo {
  cpu: number | string | null;
  memory: string | null;
  replicas: number | null;
}

export interface KubernetesStatus {
  lifecycleState: string | null;
  jobManagerDeploymentStatus: string | null;
  jobStatus: FlinkJobStatus | null;
  error: string | null;
}

export interface FlinkJobStatus {
  state: string | null;
  jobId: string | null;
  startTime: string | null;
}

export interface EventInfo {
  type: string | null;
  reason: string | null;
  message: string | null;
  timestamp: string | null;
}

export interface ErrorResponse {
  status: number;
  error: string;
  message: string;
  timestamp: string;
}
