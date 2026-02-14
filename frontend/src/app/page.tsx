"use client";

import { useEffect, useState, useCallback } from "react";
import { listJobs } from "@/lib/api";
import { JobSummary } from "@/types";
import StatusBadge from "@/components/StatusBadge";

function formatDate(dateString: string | null): string {
  if (!dateString) return "-";
  try {
    return new Date(dateString).toLocaleString();
  } catch {
    return dateString;
  }
}

export default function JobListPage() {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchJobs = useCallback(async () => {
    try {
      const data = await listJobs();
      setJobs(data);
      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to fetch jobs",
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, 5000);
    return () => clearInterval(interval);
  }, [fetchJobs]);

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Jobs</h1>
        <a
          href="/jobs/new"
          className="rounded-md bg-[var(--primary)] px-4 py-2 text-sm font-medium text-[var(--primary-foreground)] hover:opacity-90 transition-opacity"
        >
          Create Job
        </a>
      </div>

      {loading && (
        <div className="overflow-hidden rounded-lg border border-[var(--border)]">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)] bg-[var(--muted)]">
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Job Name</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Status</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Image</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Parallelism</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Created At</th>
              </tr>
            </thead>
            <tbody>
              {[...Array(3)].map((_, i) => (
                <tr key={i} className="border-b border-[var(--border)] last:border-b-0">
                  <td className="px-4 py-3"><div className="h-4 w-32 animate-pulse rounded bg-[var(--muted)]" /></td>
                  <td className="px-4 py-3"><div className="h-5 w-20 animate-pulse rounded-full bg-[var(--muted)]" /></td>
                  <td className="px-4 py-3"><div className="h-4 w-48 animate-pulse rounded bg-[var(--muted)]" /></td>
                  <td className="px-4 py-3"><div className="h-4 w-8 animate-pulse rounded bg-[var(--muted)]" /></td>
                  <td className="px-4 py-3"><div className="h-4 w-36 animate-pulse rounded bg-[var(--muted)]" /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && error && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-200">
          <p className="font-medium">Failed to load jobs</p>
          <p className="mt-1">{error}</p>
          <button
            onClick={fetchJobs}
            className="mt-3 rounded-md bg-red-100 px-3 py-1.5 text-xs font-medium text-red-800 hover:bg-red-200 dark:bg-red-900 dark:text-red-200 dark:hover:bg-red-800 transition-colors"
          >
            Retry
          </button>
        </div>
      )}

      {!loading && !error && jobs.length === 0 && (
        <div className="rounded-lg border border-[var(--border)] p-12 text-center">
          <p className="text-lg font-medium text-[var(--foreground)]">No jobs found</p>
          <p className="mt-1 text-sm text-[var(--muted-foreground)]">
            Get started by creating your first Flink CDC pipeline job.
          </p>
          <a
            href="/jobs/new"
            className="mt-4 inline-block rounded-md bg-[var(--primary)] px-4 py-2 text-sm font-medium text-[var(--primary-foreground)] hover:opacity-90 transition-opacity"
          >
            Create Job
          </a>
        </div>
      )}

      {!loading && !error && jobs.length > 0 && (
        <div className="overflow-hidden rounded-lg border border-[var(--border)]">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)] bg-[var(--muted)]">
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Job Name</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Status</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Image</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Parallelism</th>
                <th className="px-4 py-3 text-left font-medium text-[var(--muted-foreground)]">Created At</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr
                  key={`${job.namespace}/${job.jobName}`}
                  className="border-b border-[var(--border)] last:border-b-0 hover:bg-[var(--muted)] transition-colors"
                >
                  <td className="px-4 py-3">
                    <a
                      href={`/jobs/${encodeURIComponent(job.jobName)}`}
                      className="font-medium text-[var(--primary)] hover:underline"
                    >
                      {job.jobName}
                    </a>
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={job.status} />
                  </td>
                  <td className="px-4 py-3 text-[var(--muted-foreground)]">
                    {job.flinkImage ?? "-"}
                  </td>
                  <td className="px-4 py-3 text-[var(--muted-foreground)]">
                    {job.parallelism ?? "-"}
                  </td>
                  <td className="px-4 py-3 text-[var(--muted-foreground)]">
                    {formatDate(job.createdAt)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
