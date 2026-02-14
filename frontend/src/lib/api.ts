import { CreateJobRequest, JobDetail, JobSummary, ErrorResponse } from "@/types";

const API_BASE = "/api/v1";

export class ApiError extends Error {
  constructor(
    public status: number,
    public errorResponse: ErrorResponse,
  ) {
    super(errorResponse.message);
    this.name = "ApiError";
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let errorBody: ErrorResponse;
    try {
      errorBody = (await response.json()) as ErrorResponse;
    } catch {
      errorBody = {
        status: response.status,
        error: response.statusText,
        message: "An unexpected error occurred",
        timestamp: new Date().toISOString(),
      };
    }
    throw new ApiError(response.status, errorBody);
  }
  return response.json() as Promise<T>;
}

export async function createJob(request: CreateJobRequest): Promise<JobDetail> {
  const response = await fetch(`${API_BASE}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });
  return handleResponse<JobDetail>(response);
}

export async function listJobs(): Promise<JobSummary[]> {
  const response = await fetch(`${API_BASE}/jobs`);
  return handleResponse<JobSummary[]>(response);
}

export async function getJob(jobName: string): Promise<JobDetail> {
  const response = await fetch(
    `${API_BASE}/jobs/${encodeURIComponent(jobName)}`,
  );
  return handleResponse<JobDetail>(response);
}

export async function deleteJob(
  jobName: string,
): Promise<{ message: string }> {
  const response = await fetch(
    `${API_BASE}/jobs/${encodeURIComponent(jobName)}`,
    { method: "DELETE" },
  );
  return handleResponse<{ message: string }>(response);
}
