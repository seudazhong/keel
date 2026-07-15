import { useEffect } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type {
  CreateKnowledgeBaseInput,
  Job,
  KnowledgeBase,
  KnowledgeBaseDeleteResponse,
  KnowledgeDocument,
  KnowledgeDocumentDeleteResponse,
  KnowledgeDocumentDetail,
  KnowledgeDocumentJobResponse,
  KnowledgeSearchResponse,
  WriteKnowledgeDocumentInput,
} from "./types";

const knowledgeKeys = {
  bases: ["knowledge-bases"] as const,
  documents: (kbId: string) => ["knowledge-bases", kbId, "documents"] as const,
  document: (kbId: string, documentId: string) =>
    ["knowledge-bases", kbId, "documents", documentId] as const,
  job: (jobId: string) => ["jobs", jobId] as const,
  search: (kbId: string, query: string, k: number) =>
    ["knowledge-bases", kbId, "search", query, k] as const,
};

function idempotencyInit(): RequestInit {
  return { headers: { "Idempotency-Key": crypto.randomUUID() } };
}

function isTerminal(status: Job["status"]): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

export function useKnowledgeBases() {
  return useQuery({
    queryKey: knowledgeKeys.bases,
    queryFn: () => api.get<KnowledgeBase[]>("/v1/knowledge-bases"),
  });
}

export function useCreateKnowledgeBase() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: CreateKnowledgeBaseInput) =>
      api.post<KnowledgeBase>("/v1/knowledge-bases", input, idempotencyInit()),
    onSuccess: () => void qc.invalidateQueries({ queryKey: knowledgeKeys.bases }),
  });
}

export function useDeleteKnowledgeBase() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (kbId: string) =>
      api.del<KnowledgeBaseDeleteResponse>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId)}`,
        idempotencyInit(),
      ),
    onMutate: async (kbId) => {
      await qc.cancelQueries({ queryKey: knowledgeKeys.bases });
      const previous = qc.getQueryData<KnowledgeBase[]>(knowledgeKeys.bases);
      qc.setQueryData<KnowledgeBase[]>(knowledgeKeys.bases, (rows) =>
        rows?.filter((row) => row.id !== kbId),
      );
      return { previous };
    },
    onError: (_error, _kbId, context) => {
      if (context?.previous) qc.setQueryData(knowledgeKeys.bases, context.previous);
    },
    onSuccess: (response) => qc.setQueryData(knowledgeKeys.job(response.job.id), response.job),
    onSettled: () => void qc.invalidateQueries({ queryKey: knowledgeKeys.bases }),
  });
}

export function useKnowledgeDocuments(kbId: string | null) {
  return useQuery({
    queryKey: knowledgeKeys.documents(kbId ?? ""),
    queryFn: () =>
      api.get<KnowledgeDocument[]>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId ?? "")}/documents`,
      ),
    enabled: Boolean(kbId),
  });
}

export function useKnowledgeDocument(kbId: string | null, documentId: string | null) {
  return useQuery({
    queryKey: knowledgeKeys.document(kbId ?? "", documentId ?? ""),
    queryFn: () =>
      api.get<KnowledgeDocumentDetail>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId ?? "")}/documents/${encodeURIComponent(documentId ?? "")}`,
      ),
    enabled: Boolean(kbId && documentId),
  });
}

function useDocumentMutation(
  request: (
    kbId: string,
    documentId: string,
    input: WriteKnowledgeDocumentInput,
  ) => Promise<KnowledgeDocumentJobResponse>,
) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      kbId,
      documentId,
      input,
    }: {
      kbId: string;
      documentId: string;
      input: WriteKnowledgeDocumentInput;
    }) => request(kbId, documentId, input),
    onSuccess: (response) => {
      qc.setQueryData(knowledgeKeys.job(response.job.id), response.job);
      void qc.invalidateQueries({ queryKey: knowledgeKeys.documents(response.document.kb_id) });
      void qc.invalidateQueries({
        queryKey: knowledgeKeys.document(response.document.kb_id, response.document.id),
      });
    },
  });
}

export function useCreateKnowledgeDocument() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ kbId, input }: { kbId: string; input: WriteKnowledgeDocumentInput }) =>
      api.post<KnowledgeDocumentJobResponse>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId)}/documents`,
        input,
        idempotencyInit(),
      ),
    onSuccess: (response) => {
      qc.setQueryData(knowledgeKeys.job(response.job.id), response.job);
      void qc.invalidateQueries({ queryKey: knowledgeKeys.documents(response.document.kb_id) });
    },
  });
}

export function useUpdateKnowledgeDocument() {
  return useDocumentMutation((kbId, documentId, input) =>
    api.put<KnowledgeDocumentJobResponse>(
      `/v1/knowledge-bases/${encodeURIComponent(kbId)}/documents/${encodeURIComponent(documentId)}`,
      input,
      idempotencyInit(),
    ),
  );
}

export function useReindexKnowledgeDocument() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ kbId, documentId }: { kbId: string; documentId: string }) =>
      api.post<KnowledgeDocumentJobResponse>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId)}/documents/${encodeURIComponent(documentId)}/reindex`,
        {},
        idempotencyInit(),
      ),
    onSuccess: (response) => {
      qc.setQueryData(knowledgeKeys.job(response.job.id), response.job);
      void qc.invalidateQueries({ queryKey: knowledgeKeys.documents(response.document.kb_id) });
      void qc.invalidateQueries({
        queryKey: knowledgeKeys.document(response.document.kb_id, response.document.id),
      });
    },
  });
}

export function useDeleteKnowledgeDocument() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ kbId, documentId }: { kbId: string; documentId: string }) =>
      api.del<KnowledgeDocumentDeleteResponse>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId)}/documents/${encodeURIComponent(documentId)}`,
        idempotencyInit(),
      ),
    onMutate: async ({ kbId, documentId }) => {
      const key = knowledgeKeys.documents(kbId);
      await qc.cancelQueries({ queryKey: key });
      const previous = qc.getQueryData<KnowledgeDocument[]>(key);
      qc.setQueryData<KnowledgeDocument[]>(key, (rows) =>
        rows?.filter((row) => row.id !== documentId),
      );
      return { key, previous };
    },
    onError: (_error, _variables, context) => {
      if (context?.previous) qc.setQueryData(context.key, context.previous);
    },
    onSuccess: (response) => qc.setQueryData(knowledgeKeys.job(response.job.id), response.job),
    onSettled: (_data, _error, variables) =>
      void qc.invalidateQueries({ queryKey: knowledgeKeys.documents(variables.kbId) }),
  });
}

export function useKnowledgeSearch(kbId: string | null, query: string, k = 5) {
  return useQuery({
    queryKey: knowledgeKeys.search(kbId ?? "", query, k),
    queryFn: () =>
      api.get<KnowledgeSearchResponse>(
        `/v1/knowledge-bases/${encodeURIComponent(kbId ?? "")}/search?q=${encodeURIComponent(query)}&k=${k}`,
      ),
    enabled: Boolean(kbId && query.trim()),
  });
}

export function useKnowledgeJob(jobId: string | null, kbId: string | null) {
  const qc = useQueryClient();
  const query = useQuery({
    queryKey: knowledgeKeys.job(jobId ?? ""),
    queryFn: () => api.get<Job>(`/v1/jobs/${encodeURIComponent(jobId ?? "")}`),
    enabled: Boolean(jobId),
    refetchInterval: (state) => {
      const job = state.state.data;
      return job && !isTerminal(job.status) ? 1_000 : false;
    },
  });

  useEffect(() => {
    if (!kbId || !query.data || !isTerminal(query.data.status)) return;
    void qc.invalidateQueries({ queryKey: knowledgeKeys.bases });
    void qc.invalidateQueries({ queryKey: knowledgeKeys.documents(kbId) });
  }, [kbId, qc, query.data]);

  return query;
}
