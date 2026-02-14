"use client";

import { use, useEffect, useState, useCallback } from "react";
import { getJob, deleteJob, ApiError } from "@/lib/api";
import { JobDetail } from "@/types";
import StatusBadge from "@/components/StatusBadge";
import { useRouter } from "next/navigation";

function formatDate(dateString: string | null): string {
  if (!dateString) return "-";
  try {
    return new Date(dateString).toLocaleString();
  } catch {
    return dateString;
  }
}

export default function JobDetailPage({
  params,
}: {
  params: Promise<{ jobName: string }>;
}) {
  const { jobName } = use(params);
  const decodedJobName = decodeURIComponent(jobName);
  const router = useRouter();

  const [job, setJob] = useState<JobDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const fetchJob = useCallback(async () => {
    try {
      const data = await getJob(decodedJobName);
      setJob(data);
      setError(null);
      setNotFound(false);
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        setNotFound(true);
        setError(err.message);
      } else {
        setError(
          err instanceof Error ? err.message : "Failed to fetch job details",
        );
      }
    } finally {
      setLoading(false);
    }
  }, [decodedJobName]);

  useEffect(() => {
    fetchJob();
    const interval = setInterval(fetchJob, 5000);
    return () => clearInterval(interval);
  }, [fetchJob]);

  async function handleDelete() {
    if (!window.confirm(`Are you sure you want to delete job "${decodedJobName}"? This action cannot be undone.`)) {
      return;
    }
    setDeleting(true);
    try {
      await deleteJob(decodedJobName);
      router.push("/");
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to delete job",
      );
      setDeleting(false);
    }
  }

  if (loading) {
    return (
      <div>
        <a href="/" className="text-sm text-[var(--primary)] hover:underline">
          &lt; Back to Jobs
        </a>
        <div className="mt-6 flex items-center justify-center py-20">
          <span className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-[var(--muted-foreground)] border-t-transparent" />
        </div>
      </div>
    );
  }

  if (notFound) {
    return (
      <div>
        <a href="/" className="text-sm text-[var(--primary)] hover:underline">
          &lt; Back to Jobs
        </a>
        <div className="mt-6 rounded-lg border border-red-300 bg-red-50 p-6 text-center dark:border-red-800 dark:bg-red-950">
          <p className="text-lg font-medium text-red-800 dark:text-red-200">
            Job Not Found
          </p>
          <p className="mt-1 text-sm text-red-600 dark:text-red-400">
            {error || `The job "${decodedJobName}" does not exist.`}
          </p>
        </div>
      </div>
    );
  }

  if (error && !job) {
    return (
      <div>
        <a href="/" className="text-sm text-[var(--primary)] hover:underline">
          &lt; Back to Jobs
        </a>
        <div className="mt-6 rounded-lg border border-red-300 bg-red-50 p-4 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-200">
          <p className="font-medium">Failed to load job</p>
          <p className="mt-1">{error}</p>
          <button
            onClick={fetchJob}
            className="mt-3 rounded-md bg-red-100 px-3 py-1.5 text-xs font-medium text-red-800 hover:bg-red-200 dark:bg-red-900 dark:text-red-200 dark:hover:bg-red-800 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!job) return null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <a href="/" className="text-sm text-[var(--primary)] hover:underline">
          &lt; Back to Jobs
        </a>
        <div className="mt-2 flex items-center gap-3">
          <h1 className="text-2xl font-bold">{job.jobName}</h1>
          <StatusBadge status={job.status} />
        </div>
      </div>

      {/* Overview */}
      <section className="border border-[var(--border)] rounded-lg p-4 space-y-3">
        <h2 className="font-semibold text-lg">Overview</h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Namespace</p>
            <p>{job.namespace}</p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Flink Image</p>
            <p className="break-all">{job.flinkImage ?? "-"}</p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Parallelism</p>
            <p>{job.parallelism ?? "-"}</p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Created At</p>
            <p>{formatDate(job.createdAt)}</p>
          </div>
          {job.flinkUiUrl && (
            <div>
              <p className="text-sm text-[var(--muted-foreground)]">Flink UI</p>
              <a
                href={job.flinkUiUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--primary)] hover:underline"
              >
                Open Flink Dashboard
              </a>
            </div>
          )}
        </div>
      </section>

      {/* Resources */}
      <section className="border border-[var(--border)] rounded-lg p-4 space-y-3">
        <h2 className="font-semibold text-lg">Resources</h2>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
          <div className="space-y-2">
            <h3 className="text-sm font-medium text-[var(--muted-foreground)]">
              JobManager
            </h3>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <p className="text-sm text-[var(--muted-foreground)]">CPU</p>
                <p>{job.resources?.jobManager?.cpu ?? "-"}</p>
              </div>
              <div>
                <p className="text-sm text-[var(--muted-foreground)]">Memory</p>
                <p>{job.resources?.jobManager?.memory ?? "-"}</p>
              </div>
            </div>
          </div>
          <div className="space-y-2">
            <h3 className="text-sm font-medium text-[var(--muted-foreground)]">
              TaskManager
            </h3>
            <div className="grid grid-cols-3 gap-2">
              <div>
                <p className="text-sm text-[var(--muted-foreground)]">CPU</p>
                <p>{job.resources?.taskManager?.cpu ?? "-"}</p>
              </div>
              <div>
                <p className="text-sm text-[var(--muted-foreground)]">Memory</p>
                <p>{job.resources?.taskManager?.memory ?? "-"}</p>
              </div>
              <div>
                <p className="text-sm text-[var(--muted-foreground)]">Replicas</p>
                <p>{job.resources?.taskManager?.replicas ?? "-"}</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Kubernetes Status */}
      <section className="border border-[var(--border)] rounded-lg p-4 space-y-3">
        <h2 className="font-semibold text-lg">Kubernetes Status</h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">
              Lifecycle State
            </p>
            <p>{job.kubernetes?.lifecycleState ?? "-"}</p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">
              JM Deployment Status
            </p>
            <p>{job.kubernetes?.jobManagerDeploymentStatus ?? "-"}</p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Job State</p>
            <p>{job.kubernetes?.jobStatus?.state ?? "-"}</p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Job ID</p>
            <p className="break-all font-mono text-sm">
              {job.kubernetes?.jobStatus?.jobId ?? "-"}
            </p>
          </div>
          <div>
            <p className="text-sm text-[var(--muted-foreground)]">Start Time</p>
            <p>{formatDate(job.kubernetes?.jobStatus?.startTime ?? null)}</p>
          </div>
        </div>
        {job.kubernetes?.error && (
          <div className="mt-2 rounded-md border border-red-300 bg-red-50 p-3 dark:border-red-800 dark:bg-red-950">
            <p className="text-sm font-medium text-red-800 dark:text-red-200">
              Error
            </p>
            <p className="mt-1 text-sm text-red-700 dark:text-red-300">
              {job.kubernetes.error}
            </p>
          </div>
        )}
      </section>

      {/* Pipeline YAML */}
      <section className="border border-[var(--border)] rounded-lg p-4 space-y-3">
        <h2 className="font-semibold text-lg">Pipeline YAML</h2>
        {job.pipelineYaml ? (
          <pre className="bg-[var(--muted)] rounded-md p-4 overflow-x-auto font-mono text-sm whitespace-pre">
            {job.pipelineYaml}
          </pre>
        ) : (
          <p className="text-sm text-[var(--muted-foreground)]">
            No pipeline YAML available.
          </p>
        )}
      </section>

      {/* Events */}
      <section className="border border-[var(--border)] rounded-lg p-4 space-y-3">
        <h2 className="font-semibold text-lg">Events</h2>
        {job.events && job.events.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[var(--border)]">
                  <th className="px-3 py-2 text-left font-medium text-[var(--muted-foreground)]">
                    Type
                  </th>
                  <th className="px-3 py-2 text-left font-medium text-[var(--muted-foreground)]">
                    Reason
                  </th>
                  <th className="px-3 py-2 text-left font-medium text-[var(--muted-foreground)]">
                    Message
                  </th>
                  <th className="px-3 py-2 text-left font-medium text-[var(--muted-foreground)]">
                    Timestamp
                  </th>
                </tr>
              </thead>
              <tbody>
                {job.events.map((event, index) => (
                  <tr
                    key={index}
                    className={`border-b border-[var(--border)] last:border-b-0 ${
                      event.type === "Warning"
                        ? "bg-yellow-50 dark:bg-yellow-950"
                        : ""
                    }`}
                  >
                    <td
                      className={`px-3 py-2 ${
                        event.type === "Warning"
                          ? "font-medium text-yellow-700 dark:text-yellow-400"
                          : "text-[var(--muted-foreground)]"
                      }`}
                    >
                      {event.type ?? "-"}
                    </td>
                    <td className="px-3 py-2">{event.reason ?? "-"}</td>
                    <td className="px-3 py-2">{event.message ?? "-"}</td>
                    <td className="px-3 py-2 text-[var(--muted-foreground)]">
                      {formatDate(event.timestamp)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-sm text-[var(--muted-foreground)]">
            No events recorded.
          </p>
        )}
      </section>

      {/* Delete */}
      <section className="border border-[var(--border)] rounded-lg p-4">
        <h2 className="font-semibold text-lg text-red-600 dark:text-red-400">
          Danger Zone
        </h2>
        <p className="mt-1 text-sm text-[var(--muted-foreground)]">
          Deleting this job will remove the Flink CDC pipeline from the
          Kubernetes cluster. This action cannot be undone.
        </p>
        <button
          onClick={handleDelete}
          disabled={deleting}
          className="mt-3 rounded-md bg-[var(--destructive)] px-4 py-2 text-sm font-medium text-[var(--destructive-foreground)] hover:opacity-90 transition-opacity disabled:opacity-50"
        >
          {deleting ? "Deleting..." : "Delete Job"}
        </button>
      </section>
    </div>
  );
}
