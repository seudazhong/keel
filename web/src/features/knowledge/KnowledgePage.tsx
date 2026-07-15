import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import {
  useCreateKnowledgeBase,
  useCreateKnowledgeDocument,
  useDeleteKnowledgeBase,
  useDeleteKnowledgeDocument,
  useKnowledgeBases,
  useKnowledgeDocument,
  useKnowledgeDocuments,
  useKnowledgeJob,
  useKnowledgeSearch,
  useReindexKnowledgeDocument,
  useUpdateKnowledgeDocument,
} from "./useKnowledge";
import type {
  Job,
  KnowledgeDocument,
  KnowledgeSearchMode,
  KnowledgeSourceType,
  WriteKnowledgeDocumentInput,
} from "./types";

const inputClass =
  "w-full rounded-sm border border-border bg-surface px-3 py-2 text-sm outline-none focus:border-accent";

function statusTone(status: KnowledgeDocument["status"] | Job["status"]) {
  if (status === "active" || status === "succeeded") return "green" as const;
  if (status === "failed" || status === "cancelled") return "red" as const;
  return "amber" as const;
}

function modeTone(mode: KnowledgeSearchMode) {
  return mode === "hybrid" ? ("green" as const) : mode === "lexical" ? ("sky" as const) : ("amber" as const);
}

function sourceHref(sourceUri: string | null): string | null {
  if (!sourceUri) return null;
  try {
    const url = new URL(sourceUri);
    return url.protocol === "http:" || url.protocol === "https:" ? sourceUri : null;
  } catch {
    return null;
  }
}

function boundedError(isError: boolean) {
  return isError ? (
    <Banner tone="danger">
      <span>⚠️</span>
      <span>Request failed. Check your input or try again.</span>
    </Banner>
  ) : null;
}

function JobStatusCard({ job }: { job: Job }) {
  const progress =
    job.progress_total && job.progress_total > 0
      ? Math.min(100, Math.round((job.progress_current / job.progress_total) * 100))
      : null;
  return (
    <Card className="p-4" aria-label="Job status">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-xs text-text-muted">{job.kind}</div>
          <div className="mt-1 text-sm font-semibold">{job.progress_message ?? "Job queued"}</div>
        </div>
        <Badge tone={statusTone(job.status)}>{job.status}</Badge>
      </div>
      {progress !== null && (
        <div className="mt-3">
          <div className="mb-1 flex justify-between text-xs text-text-muted">
            <span>
              {job.progress_current} / {job.progress_total}
            </span>
            <span>{progress}%</span>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-surface-2">
            <div className="h-full bg-accent" style={{ width: `${progress}%` }} />
          </div>
        </div>
      )}
      {(job.error_message || job.result_message) && (
        <p className="mt-2 text-xs text-text-muted">{job.error_message ?? job.result_message}</p>
      )}
    </Card>
  );
}

function DocumentForm({
  title,
  initial,
  submitLabel,
  pending,
  onSubmit,
}: {
  title: string;
  initial?: WriteKnowledgeDocumentInput;
  submitLabel: string;
  pending: boolean;
  onSubmit: (input: WriteKnowledgeDocumentInput) => Promise<void>;
}) {
  const [documentTitle, setDocumentTitle] = useState(initial?.title ?? "");
  const [sourceType, setSourceType] = useState<KnowledgeSourceType>(
    initial?.source_type ?? "markdown",
  );
  const [sourceUri, setSourceUri] = useState(initial?.source_uri ?? "");
  const [content, setContent] = useState(initial?.content ?? "");

  useEffect(() => {
    setDocumentTitle(initial?.title ?? "");
    setSourceType(initial?.source_type ?? "markdown");
    setSourceUri(initial?.source_uri ?? "");
    setContent(initial?.content ?? "");
  }, [initial]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!documentTitle.trim() || !content.trim()) return;
    await onSubmit({
      title: documentTitle.trim(),
      source_type: sourceType,
      source_uri: sourceUri.trim() || null,
      content,
    });
    if (!initial) {
      setDocumentTitle("");
      setSourceUri("");
      setContent("");
    }
  }

  return (
    <Card className="p-4">
      <h3 className="mb-3 text-sm font-semibold">{title}</h3>
      <form className="space-y-3" onSubmit={(event) => void submit(event)}>
        <label className="block text-xs font-medium text-text-soft">
          Title
          <input
            className={`${inputClass} mt-1`}
            value={documentTitle}
            maxLength={300}
            onChange={(event) => setDocumentTitle(event.target.value)}
          />
        </label>
        <div className="grid grid-cols-2 gap-3">
          <label className="block text-xs font-medium text-text-soft">
            Format
            <select
              className={`${inputClass} mt-1`}
              value={sourceType}
              onChange={(event) => setSourceType(event.target.value as KnowledgeSourceType)}
            >
              <option value="markdown">Markdown</option>
              <option value="text">Text</option>
            </select>
          </label>
          <label className="block text-xs font-medium text-text-soft">
            Source URL (optional)
            <input
              className={`${inputClass} mt-1`}
              type="url"
              value={sourceUri}
              onChange={(event) => setSourceUri(event.target.value)}
            />
          </label>
        </div>
        <label className="block text-xs font-medium text-text-soft">
          Content
          <textarea
            className={`${inputClass} mt-1 min-h-36 resize-y font-mono`}
            value={content}
            onChange={(event) => setContent(event.target.value)}
          />
        </label>
        <Button
          variant="primary"
          type="submit"
          disabled={pending || !documentTitle.trim() || !content.trim()}
        >
          {pending ? "Submitting…" : submitLabel}
        </Button>
      </form>
    </Card>
  );
}

export function KnowledgePage() {
  const bases = useKnowledgeBases();
  const createBase = useCreateKnowledgeBase();
  const deleteBase = useDeleteKnowledgeBase();
  const [selectedKbId, setSelectedKbId] = useState<string | null>(null);
  const [selectedDocumentId, setSelectedDocumentId] = useState<string | null>(null);
  const [baseName, setBaseName] = useState("");
  const [baseDescription, setBaseDescription] = useState("");
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const documents = useKnowledgeDocuments(selectedKbId);
  const detail = useKnowledgeDocument(selectedKbId, selectedDocumentId);
  const createDocument = useCreateKnowledgeDocument();
  const updateDocument = useUpdateKnowledgeDocument();
  const reindexDocument = useReindexKnowledgeDocument();
  const deleteDocument = useDeleteKnowledgeDocument();
  const job = useKnowledgeJob(activeJobId, selectedKbId);
  const [searchInput, setSearchInput] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const search = useKnowledgeSearch(selectedKbId, searchQuery);

  useEffect(() => {
    if (!bases.data) return;
    if (selectedKbId && bases.data.some((base) => base.id === selectedKbId)) return;
    setSelectedKbId(bases.data[0]?.id ?? null);
    setSelectedDocumentId(null);
    setSearchQuery("");
  }, [bases.data, selectedKbId]);

  const selectedBase = bases.data?.find((base) => base.id === selectedKbId);
  const editableVersion = useMemo(() => {
    if (!detail.data) return undefined;
    return (
      detail.data.versions.find(
        (version) => version.id === detail.data.document.desired_version_id,
      ) ??
      detail.data.versions.find((version) => version.id === detail.data.document.active_version_id) ??
      detail.data.versions[0]
    );
  }, [detail.data]);
  const editInput = useMemo(
    () =>
      detail.data
        ? {
            title: detail.data.document.title,
            source_type: detail.data.document.source_type,
            source_uri: detail.data.document.source_uri,
            content: editableVersion?.content ?? "",
          }
        : undefined,
    [detail.data, editableVersion],
  );
  const mutationFailed =
    createBase.isError ||
    deleteBase.isError ||
    createDocument.isError ||
    updateDocument.isError ||
    reindexDocument.isError ||
    deleteDocument.isError;

  async function addBase(event: FormEvent) {
    event.preventDefault();
    if (!baseName.trim()) return;
    try {
      const created = await createBase.mutateAsync({
        name: baseName.trim(),
        description: baseDescription.trim() || null,
      });
      setSelectedKbId(created.id);
      setBaseName("");
      setBaseDescription("");
    } catch {
      return;
    }
  }

  async function addDocument(input: WriteKnowledgeDocumentInput) {
    if (!selectedKbId) return;
    try {
      const response = await createDocument.mutateAsync({ kbId: selectedKbId, input });
      setSelectedDocumentId(response.document.id);
      setActiveJobId(response.job.id);
    } catch {
      return;
    }
  }

  async function updateSelectedDocument(input: WriteKnowledgeDocumentInput) {
    if (!selectedKbId || !selectedDocumentId) return;
    try {
      const response = await updateDocument.mutateAsync({
        kbId: selectedKbId,
        documentId: selectedDocumentId,
        input,
      });
      setActiveJobId(response.job.id);
    } catch {
      return;
    }
  }

  async function reindexSelectedDocument() {
    if (!selectedKbId || !selectedDocumentId) return;
    try {
      const response = await reindexDocument.mutateAsync({
        kbId: selectedKbId,
        documentId: selectedDocumentId,
      });
      setActiveJobId(response.job.id);
    } catch {
      return;
    }
  }

  async function removeSelectedDocument() {
    if (!selectedKbId || !selectedDocumentId) return;
    const documentId = selectedDocumentId;
    setSelectedDocumentId(null);
    try {
      const response = await deleteDocument.mutateAsync({ kbId: selectedKbId, documentId });
      setActiveJobId(response.job.id);
    } catch {
      setSelectedDocumentId(documentId);
    }
  }

  return (
    <>
      <Topbar
        title="Knowledge"
        sub="· Manage scoped knowledge bases, documents, and retrieval"
        right={<Badge tone="violet">scope: personal · web:local</Badge>}
      />
      <div className="space-y-4 p-[22px]">
        {boundedError(bases.isError || documents.isError || detail.isError || job.isError || mutationFailed)}
        <div className="grid gap-4 xl:grid-cols-[260px_minmax(0,1fr)_minmax(320px,0.8fr)]">
          <section className="space-y-3" aria-label="Knowledge bases">
            <div className="text-sm font-semibold text-text-soft">Knowledge bases</div>
            {bases.isLoading && <Skeleton className="h-32" />}
            {bases.data && bases.data.length === 0 && (
              <Card className="p-4 text-sm text-text-muted">
                No knowledge bases yet. Create one to add documents.
              </Card>
            )}
            {bases.data?.map((base) => (
              <button
                key={base.id}
                className={`w-full rounded border p-3 text-left ${
                  base.id === selectedKbId
                    ? "border-accent bg-accent/5"
                    : "border-border bg-surface hover:bg-surface-2"
                }`}
                onClick={() => {
                  setSelectedKbId(base.id);
                  setSelectedDocumentId(null);
                  setSearchQuery("");
                }}
              >
                <span className="block text-sm font-semibold">{base.name}</span>
                <span className="mt-1 block line-clamp-2 text-xs text-text-muted">
                  {base.description || `${base.embedding_model} · ${base.embedding_dim}d`}
                </span>
              </button>
            ))}
            <Card className="p-3">
              <form className="space-y-2" onSubmit={(event) => void addBase(event)}>
                <label className="block text-xs font-medium text-text-soft">
                  New knowledge base
                  <input
                    className={`${inputClass} mt-1`}
                    aria-label="Knowledge base name"
                    placeholder="Name"
                    value={baseName}
                    maxLength={300}
                    onChange={(event) => setBaseName(event.target.value)}
                  />
                </label>
                <textarea
                  className={`${inputClass} min-h-16 resize-y`}
                  aria-label="Knowledge base description"
                  placeholder="Description (optional)"
                  value={baseDescription}
                  maxLength={2000}
                  onChange={(event) => setBaseDescription(event.target.value)}
                />
                <Button
                  className="w-full justify-center"
                  variant="primary"
                  type="submit"
                  disabled={createBase.isPending || !baseName.trim()}
                >
                  Create KB
                </Button>
              </form>
            </Card>
            {selectedBase && (
              <Button
                className="w-full justify-center"
                variant="danger"
                disabled={deleteBase.isPending}
                onClick={() => {
                  const kbId = selectedBase.id;
                  const deletion = deleteBase.mutateAsync(kbId);
                  setSelectedKbId(null);
                  setSelectedDocumentId(null);
                  void deletion
                    .then((response) => setActiveJobId(response.job.id))
                    .catch(() => setSelectedKbId(kbId));
                }}
              >
                Delete KB
              </Button>
            )}
          </section>

          <section className="space-y-3" aria-label="Documents">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold text-text-soft">
                Documents{selectedBase ? ` · ${selectedBase.name}` : ""}
              </div>
              {documents.data && <Badge>{documents.data.length}</Badge>}
            </div>
            {!selectedKbId && !bases.isLoading && (
              <Card className="p-5 text-sm text-text-muted">Select or create a knowledge base.</Card>
            )}
            {selectedKbId && documents.isLoading && <Skeleton className="h-36" />}
            {selectedKbId && documents.data?.length === 0 && (
              <Card className="p-5 text-sm text-text-muted">
                This knowledge base has no documents. Add text or Markdown below.
              </Card>
            )}
            {documents.data?.map((document) => (
              <Card
                key={document.id}
                className={`p-4 ${document.id === selectedDocumentId ? "border-accent" : ""}`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <h3 className="truncate text-sm font-semibold">{document.title}</h3>
                    <div className="mt-1 text-xs text-text-muted">
                      {document.source_type}
                      {document.active_version_id ? ` · active version ${document.active_version_id}` : ""}
                    </div>
                    {document.last_error_message && (
                      <p className="mt-2 line-clamp-2 text-xs text-red">
                        {document.last_error_message}
                      </p>
                    )}
                  </div>
                  <Badge tone={statusTone(document.status)}>{document.status}</Badge>
                </div>
                <Button
                  className="mt-3 px-2.5 py-1 text-xs"
                  onClick={() => setSelectedDocumentId(document.id)}
                >
                  Manage
                </Button>
              </Card>
            ))}
            {selectedKbId && (
              <DocumentForm
                title="Add document"
                submitLabel="Add and index"
                pending={createDocument.isPending}
                onSubmit={addDocument}
              />
            )}
            {selectedDocumentId && detail.isLoading && <Skeleton className="h-64" />}
            {selectedDocumentId && editInput && (
              <>
                <DocumentForm
                  key={selectedDocumentId}
                  title={`Update · ${detail.data?.document.title ?? ""}`}
                  initial={editInput}
                  submitLabel="Save new version"
                  pending={updateDocument.isPending}
                  onSubmit={updateSelectedDocument}
                />
                <div className="flex flex-wrap gap-2">
                  <Button
                    disabled={reindexDocument.isPending}
                    onClick={() => void reindexSelectedDocument()}
                  >
                    Reindex
                  </Button>
                  <Button
                    variant="danger"
                    disabled={deleteDocument.isPending}
                    onClick={() => void removeSelectedDocument()}
                  >
                    Delete document
                  </Button>
                </div>
              </>
            )}
            {activeJobId && job.isLoading && <Skeleton className="h-24" />}
            {job.data && <JobStatusCard job={job.data} />}
          </section>

          <section className="space-y-3" aria-label="Knowledge search">
            <div className="text-sm font-semibold text-text-soft">Search</div>
            <Card className="p-4">
              <form
                className="flex gap-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  setSearchQuery(searchInput.trim());
                }}
              >
                <input
                  className={inputClass}
                  aria-label="Search knowledge"
                  placeholder="Search this knowledge base"
                  value={searchInput}
                  maxLength={2000}
                  disabled={!selectedKbId}
                  onChange={(event) => setSearchInput(event.target.value)}
                />
                <Button
                  variant="primary"
                  type="submit"
                  disabled={!selectedKbId || !searchInput.trim()}
                >
                  Search
                </Button>
              </form>
            </Card>
            {search.isLoading && <Skeleton className="h-32" />}
            {search.isError && boundedError(true)}
            {search.data && (
              <>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-text-muted">{search.data.hits.length} results</span>
                  <Badge tone={modeTone(search.data.status.mode)}>
                    {search.data.status.mode}
                  </Badge>
                </div>
                {search.data.status.semantic_error && (
                  <Banner tone="warn">Semantic search unavailable; showing lexical results.</Banner>
                )}
                {search.data.hits.length === 0 && (
                  <Card className="p-5 text-sm text-text-muted">
                    No matching passages were found.
                  </Card>
                )}
                {search.data.hits.map((hit) => {
                  const href = sourceHref(hit.citation.source_uri);
                  return (
                    <Card key={hit.citation.id} className="p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <h3 className="text-sm font-semibold">
                            {href ? (
                              <a
                                className="text-accent underline"
                                href={href}
                                target="_blank"
                                rel="noreferrer"
                              >
                                {hit.citation.title}
                              </a>
                            ) : (
                              hit.citation.title
                            )}
                          </h3>
                          {hit.heading_path.length > 0 && (
                            <div className="mt-1 text-xs text-text-muted">
                              {hit.heading_path.join(" › ")}
                            </div>
                          )}
                        </div>
                        <Badge>#{hit.rank}</Badge>
                      </div>
                      <p className="mt-3 whitespace-pre-wrap text-sm leading-6 text-text-soft">
                        {hit.snippet}
                      </p>
                      <div className="mt-3 border-t border-border pt-2 text-xs text-text-muted">
                        {hit.citation.label} · chunk {hit.citation.ordinal} · chars{" "}
                        {hit.citation.char_start}–{hit.citation.char_end}
                      </div>
                    </Card>
                  );
                })}
              </>
            )}
          </section>
        </div>
      </div>
    </>
  );
}
