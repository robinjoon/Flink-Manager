"use client";

import { useState } from "react";
import { createJob, ApiError } from "@/lib/api";
import type { CreateJobRequest } from "@/types";

const JOB_NAME_PATTERN = /^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?$/;

interface FieldErrors {
  jobName?: string;
  pipelineYaml?: string;
  flinkImage?: string;
  parallelism?: string;
  jmCpu?: string;
  tmCpu?: string;
  tmReplicas?: string;
}

export default function NewJobPage() {
  const [jobName, setJobName] = useState("");
  const [pipelineYaml, setPipelineYaml] = useState("");
  const [flinkImage, setFlinkImage] = useState("");

  const [jmCpu, setJmCpu] = useState(1);
  const [jmMemory, setJmMemory] = useState("1024m");
  const [tmCpu, setTmCpu] = useState(1);
  const [tmMemory, setTmMemory] = useState("2048m");
  const [tmReplicas, setTmReplicas] = useState(1);
  const [parallelism, setParallelism] = useState(1);

  const [flinkVersion, setFlinkVersion] = useState("");
  const [serviceAccount, setServiceAccount] = useState("");
  const [namespace, setNamespace] = useState("");

  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const [fieldErrors, setFieldErrors] = useState<FieldErrors>({});

  function validate(): FieldErrors {
    const errors: FieldErrors = {};

    if (!jobName.trim()) {
      errors.jobName = "Job name is required.";
    } else if (!JOB_NAME_PATTERN.test(jobName)) {
      errors.jobName =
        "Must start and end with a lowercase letter or number. Only lowercase letters, numbers, and hyphens allowed. Max 63 characters.";
    }

    if (!pipelineYaml.trim()) {
      errors.pipelineYaml = "Pipeline YAML is required.";
    }

    if (!flinkImage.trim()) {
      errors.flinkImage = "Flink image is required.";
    }

    if (parallelism < 1) {
      errors.parallelism = "Parallelism must be at least 1.";
    }

    if (jmCpu < 1) {
      errors.jmCpu = "CPU must be at least 1.";
    }

    if (tmCpu < 1) {
      errors.tmCpu = "CPU must be at least 1.";
    }

    if (tmReplicas < 1) {
      errors.tmReplicas = "Replicas must be at least 1.";
    }

    return errors;
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setApiError(null);

    const errors = validate();
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) {
      return;
    }

    const request: CreateJobRequest = {
      jobName: jobName.trim(),
      pipelineYaml: pipelineYaml,
      flinkImage: flinkImage.trim(),
      resources: {
        jobManager: { cpu: jmCpu, memory: jmMemory },
        taskManager: { cpu: tmCpu, memory: tmMemory, replicas: tmReplicas },
      },
      parallelism,
    };

    if (flinkVersion.trim() || serviceAccount.trim()) {
      request.flink = {};
      if (flinkVersion.trim()) request.flink.version = flinkVersion.trim();
      if (serviceAccount.trim())
        request.flink.serviceAccount = serviceAccount.trim();
    }

    if (namespace.trim()) {
      request.namespace = namespace.trim();
    }

    setSubmitting(true);
    try {
      await createJob(request);
      window.location.href = `/jobs/${encodeURIComponent(request.jobName)}`;
    } catch (err) {
      if (err instanceof ApiError) {
        setApiError(err.errorResponse.message);
      } else {
        setApiError("An unexpected error occurred.");
      }
      setSubmitting(false);
    }
  }

  return (
    <div className="max-w-3xl mx-auto">
      <h1 className="text-2xl font-bold mb-6">Create New Job</h1>

      {apiError && (
        <div className="mb-6 rounded-md border border-[var(--destructive)] bg-[var(--destructive)]/10 px-4 py-3 text-sm text-[var(--destructive)]">
          {apiError}
        </div>
      )}

      <form onSubmit={handleSubmit} noValidate>
        {/* Basic Settings */}
        <section className="mb-8">
          <h2 className="text-lg font-semibold mb-4">Basic Settings</h2>

          <div className="mb-4">
            <label
              htmlFor="jobName"
              className="block text-sm font-medium mb-1"
            >
              Job Name <span className="text-[var(--destructive)]">*</span>
            </label>
            <input
              id="jobName"
              type="text"
              required
              maxLength={63}
              value={jobName}
              onChange={(e) => setJobName(e.target.value)}
              className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
            />
            {fieldErrors.jobName ? (
              <p className="mt-1 text-sm text-[var(--destructive)]">
                {fieldErrors.jobName}
              </p>
            ) : (
              <p className="mt-1 text-sm text-[var(--muted-foreground)]">
                Lowercase letters, numbers, hyphens. Max 63 chars.
              </p>
            )}
          </div>

          <div className="mb-4">
            <label
              htmlFor="pipelineYaml"
              className="block text-sm font-medium mb-1"
            >
              Pipeline YAML <span className="text-[var(--destructive)]">*</span>
            </label>
            <textarea
              id="pipelineYaml"
              required
              rows={12}
              value={pipelineYaml}
              onChange={(e) => setPipelineYaml(e.target.value)}
              className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-[var(--primary)] resize-y"
            />
            {fieldErrors.pipelineYaml && (
              <p className="mt-1 text-sm text-[var(--destructive)]">
                {fieldErrors.pipelineYaml}
              </p>
            )}
          </div>

          <div className="mb-4">
            <label
              htmlFor="flinkImage"
              className="block text-sm font-medium mb-1"
            >
              Flink Image <span className="text-[var(--destructive)]">*</span>
            </label>
            <input
              id="flinkImage"
              type="text"
              required
              placeholder="e.g., my-registry/flink-cdc:3.3.0"
              value={flinkImage}
              onChange={(e) => setFlinkImage(e.target.value)}
              className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
            />
            {fieldErrors.flinkImage && (
              <p className="mt-1 text-sm text-[var(--destructive)]">
                {fieldErrors.flinkImage}
              </p>
            )}
          </div>
        </section>

        {/* Resource Settings */}
        <section className="mb-8">
          <h2 className="text-lg font-semibold mb-4">Resource Settings</h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
            {/* JobManager */}
            <div className="rounded-md border border-[var(--border)] p-4">
              <h3 className="text-sm font-semibold mb-3">JobManager</h3>

              <div className="mb-3">
                <label
                  htmlFor="jmCpu"
                  className="block text-sm font-medium mb-1"
                >
                  CPU
                </label>
                <input
                  id="jmCpu"
                  type="number"
                  min={1}
                  value={jmCpu}
                  onChange={(e) => setJmCpu(Number(e.target.value))}
                  className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
                {fieldErrors.jmCpu && (
                  <p className="mt-1 text-sm text-[var(--destructive)]">
                    {fieldErrors.jmCpu}
                  </p>
                )}
              </div>

              <div>
                <label
                  htmlFor="jmMemory"
                  className="block text-sm font-medium mb-1"
                >
                  Memory
                </label>
                <input
                  id="jmMemory"
                  type="text"
                  value={jmMemory}
                  onChange={(e) => setJmMemory(e.target.value)}
                  className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
              </div>
            </div>

            {/* TaskManager */}
            <div className="rounded-md border border-[var(--border)] p-4">
              <h3 className="text-sm font-semibold mb-3">TaskManager</h3>

              <div className="mb-3">
                <label
                  htmlFor="tmCpu"
                  className="block text-sm font-medium mb-1"
                >
                  CPU
                </label>
                <input
                  id="tmCpu"
                  type="number"
                  min={1}
                  value={tmCpu}
                  onChange={(e) => setTmCpu(Number(e.target.value))}
                  className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
                {fieldErrors.tmCpu && (
                  <p className="mt-1 text-sm text-[var(--destructive)]">
                    {fieldErrors.tmCpu}
                  </p>
                )}
              </div>

              <div className="mb-3">
                <label
                  htmlFor="tmMemory"
                  className="block text-sm font-medium mb-1"
                >
                  Memory
                </label>
                <input
                  id="tmMemory"
                  type="text"
                  value={tmMemory}
                  onChange={(e) => setTmMemory(e.target.value)}
                  className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
              </div>

              <div>
                <label
                  htmlFor="tmReplicas"
                  className="block text-sm font-medium mb-1"
                >
                  Replicas
                </label>
                <input
                  id="tmReplicas"
                  type="number"
                  min={1}
                  value={tmReplicas}
                  onChange={(e) => setTmReplicas(Number(e.target.value))}
                  className="w-full border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
                {fieldErrors.tmReplicas && (
                  <p className="mt-1 text-sm text-[var(--destructive)]">
                    {fieldErrors.tmReplicas}
                  </p>
                )}
              </div>
            </div>
          </div>

          <div className="mb-4">
            <label
              htmlFor="parallelism"
              className="block text-sm font-medium mb-1"
            >
              Parallelism
            </label>
            <input
              id="parallelism"
              type="number"
              min={1}
              value={parallelism}
              onChange={(e) => setParallelism(Number(e.target.value))}
              className="w-full max-w-xs border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
            />
            {fieldErrors.parallelism && (
              <p className="mt-1 text-sm text-[var(--destructive)]">
                {fieldErrors.parallelism}
              </p>
            )}
          </div>
        </section>

        {/* Advanced Settings */}
        <section className="mb-8">
          <button
            type="button"
            onClick={() => setAdvancedOpen(!advancedOpen)}
            className="flex items-center gap-2 text-lg font-semibold mb-4 hover:text-[var(--primary)] transition-colors"
          >
            <svg
              className={`w-4 h-4 transition-transform ${advancedOpen ? "rotate-90" : ""}`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M9 5l7 7-7 7"
              />
            </svg>
            Advanced Settings
          </button>

          {advancedOpen && (
            <div className="space-y-4 pl-6">
              <div>
                <label
                  htmlFor="flinkVersion"
                  className="block text-sm font-medium mb-1"
                >
                  Flink Version
                </label>
                <input
                  id="flinkVersion"
                  type="text"
                  placeholder="v1_18"
                  value={flinkVersion}
                  onChange={(e) => setFlinkVersion(e.target.value)}
                  className="w-full max-w-sm border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
              </div>

              <div>
                <label
                  htmlFor="serviceAccount"
                  className="block text-sm font-medium mb-1"
                >
                  Service Account
                </label>
                <input
                  id="serviceAccount"
                  type="text"
                  placeholder="flink"
                  value={serviceAccount}
                  onChange={(e) => setServiceAccount(e.target.value)}
                  className="w-full max-w-sm border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
              </div>

              <div>
                <label
                  htmlFor="namespace"
                  className="block text-sm font-medium mb-1"
                >
                  Namespace
                </label>
                <input
                  id="namespace"
                  type="text"
                  placeholder="flink-jobs"
                  value={namespace}
                  onChange={(e) => setNamespace(e.target.value)}
                  className="w-full max-w-sm border border-[var(--border)] bg-[var(--background)] rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--primary)]"
                />
              </div>
            </div>
          )}
        </section>

        {/* Buttons */}
        <div className="flex items-center gap-3 pt-4 border-t border-[var(--border)]">
          <button
            type="submit"
            disabled={submitting}
            className="bg-[var(--primary)] text-[var(--primary-foreground)] rounded-md px-4 py-2 text-sm font-medium hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {submitting ? "Creating..." : "Create Job"}
          </button>
          <a
            href="/"
            className="border border-[var(--border)] rounded-md px-4 py-2 text-sm font-medium hover:bg-[var(--muted)] transition-colors"
          >
            Cancel
          </a>
        </div>
      </form>
    </div>
  );
}
