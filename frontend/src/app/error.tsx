"use client";
export default function ErrorPage({ reset }: { reset: () => void }) {
  return (
    <main className="flex min-h-dvh items-center justify-center p-6">
      <section role="alert" className="w-full max-w-md border border-border bg-surface p-6">
        <h1 className="font-display text-2xl">Unable to display the graph</h1>
        <p className="my-4 text-sm text-muted">
          Reload the view to try again. Your stored graph has not been changed.
        </p>
        <button onClick={reset}>Retry</button>
      </section>
    </main>
  );
}
