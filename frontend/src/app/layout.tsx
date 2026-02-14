import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Flink CDC Admin",
  description: "Flink CDC Pipeline 관리 도구",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ko">
      <body className="min-h-screen bg-[var(--background)] text-[var(--foreground)]">
        <header className="sticky top-0 z-50 border-b border-[var(--border)] bg-[var(--background)]">
          <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4">
            <a href="/" className="text-lg font-bold">
              Flink CDC Admin
            </a>
            <nav className="flex items-center gap-4">
              <a
                href="/"
                className="text-sm text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
              >
                Jobs
              </a>
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-4 py-6">
          {children}
        </main>
      </body>
    </html>
  );
}
