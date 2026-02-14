"use client";

import { JobStatus } from "@/types";

const STATUS_CONFIG: Record<
  JobStatus,
  { label: string; className: string }
> = {
  [JobStatus.DEPLOYING]: {
    label: "Deploying",
    className: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
  },
  [JobStatus.RUNNING]: {
    label: "Running",
    className:
      "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  },
  [JobStatus.FAILED]: {
    label: "Failed",
    className: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
  },
  [JobStatus.SUSPENDED]: {
    label: "Suspended",
    className: "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200",
  },
  [JobStatus.UPGRADING]: {
    label: "Upgrading",
    className: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
  },
  [JobStatus.UNKNOWN]: {
    label: "Unknown",
    className: "bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400",
  },
};

export default function StatusBadge({ status }: { status: JobStatus }) {
  const config = STATUS_CONFIG[status] ?? STATUS_CONFIG[JobStatus.UNKNOWN];

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ${config.className}`}
    >
      {(status === JobStatus.DEPLOYING || status === JobStatus.UPGRADING) && (
        <span className="inline-block h-2 w-2 animate-spin rounded-full border border-current border-t-transparent" />
      )}
      {status === JobStatus.RUNNING && (
        <span className="inline-block h-2 w-2 rounded-full bg-green-500 dark:bg-green-400" />
      )}
      {status === JobStatus.FAILED && (
        <span className="inline-block h-2 w-2 rounded-full bg-red-500 dark:bg-red-400" />
      )}
      {config.label}
    </span>
  );
}
