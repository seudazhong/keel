import { fireEvent, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { useState } from "react";
import { expect, test } from "vitest";
import { knowledgeIdempotencyKeys, sampleKnowledgeBase } from "../../test/handlers";
import { server } from "../../test/setup";
import { renderWithClient } from "../../test/utils";
import { KnowledgePage } from "./KnowledgePage";
import {
  useCreateKnowledgeBase,
  useCreateKnowledgeDocument,
  useDeleteKnowledgeBase,
  useDeleteKnowledgeDocument,
  useReindexKnowledgeDocument,
  useUpdateKnowledgeDocument,
} from "./useKnowledge";

test("renders bounded empty and error states", async () => {
  server.use(http.get("/v1/knowledge-bases", () => HttpResponse.json([])));
  const { unmount } = renderWithClient(<KnowledgePage />);
  expect(await screen.findByText(/No knowledge bases yet/)).toBeInTheDocument();
  unmount();

  server.use(
    http.get("/v1/knowledge-bases", () => HttpResponse.json({ detail: "private" }, { status: 500 })),
  );
  renderWithClient(<KnowledgePage />);
  expect(await screen.findByText("Request failed. Check your input or try again.")).toBeInTheDocument();
  expect(screen.queryByText("private")).not.toBeInTheDocument();
});

test("creates a KB and exposes document lifecycle actions with real job status", async () => {
  renderWithClient(<KnowledgePage />);
  expect(await screen.findByText("Refund policy")).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("Knowledge base name"), {
    target: { value: "Support notes" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Create KB" }));
  expect(await screen.findByRole("button", { name: /Support notes/ })).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("Title"), { target: { value: "Escalation guide" } });
  fireEvent.change(screen.getByLabelText("Content"), {
    target: { value: "# Escalation\nContact the on-call engineer." },
  });
  fireEvent.click(screen.getByRole("button", { name: "Add and index" }));
  expect(await screen.findByText("Escalation guide")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: /Product handbook/ }));
  fireEvent.click(await screen.findByRole("button", { name: "Manage" }));
  expect(await screen.findByRole("button", { name: "Reindex" })).toBeInTheDocument();
  const editContent = screen.getAllByLabelText("Content")[1];
  fireEvent.change(editContent, { target: { value: "# Updated policy" } });
  expect(editContent).toHaveValue("# Updated policy");
  fireEvent.click(screen.getByRole("button", { name: "Save new version" }));
  expect(await screen.findByText("Indexed document")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Reindex" }));
  await waitFor(() => expect(knowledgeIdempotencyKeys).toHaveLength(4));

  fireEvent.click(screen.getByRole("button", { name: "Delete document" }));
  await waitFor(() => expect(screen.queryByText("Refund policy")).not.toBeInTheDocument());
});

test("renders retrieval mode, snippets, structured citations, and source links", async () => {
  renderWithClient(<KnowledgePage />);
  await screen.findByText("Refund policy");
  fireEvent.change(screen.getByLabelText("Search knowledge"), {
    target: { value: "refund window" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Search" }));

  expect(await screen.findByText("lexical-degraded")).toBeInTheDocument();
  expect(screen.getByText(/Customers may request a refund/)).toBeInTheDocument();
  expect(screen.getByText(/chunk 2 · chars 120–184/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Refund policy" })).toHaveAttribute(
    "href",
    "https://example.com/handbook/refunds",
  );
});

function MutationHarness() {
  const [status, setStatus] = useState("idle");
  const createBase = useCreateKnowledgeBase();
  const deleteBase = useDeleteKnowledgeBase();
  const createDocument = useCreateKnowledgeDocument();
  const updateDocument = useUpdateKnowledgeDocument();
  const reindexDocument = useReindexKnowledgeDocument();
  const deleteDocument = useDeleteKnowledgeDocument();

  async function run() {
    const base = await createBase.mutateAsync({ name: "Idempotency test" });
    const created = await createDocument.mutateAsync({
      kbId: base.id,
      input: { title: "Doc", source_type: "text", content: "Version one" },
    });
    await updateDocument.mutateAsync({
      kbId: base.id,
      documentId: created.document.id,
      input: { title: "Doc v2", source_type: "text", content: "Version two" },
    });
    await reindexDocument.mutateAsync({ kbId: base.id, documentId: created.document.id });
    await deleteDocument.mutateAsync({ kbId: base.id, documentId: created.document.id });
    await deleteBase.mutateAsync(base.id);
    setStatus("done");
  }

  return <button onClick={() => void run()}>{status}</button>;
}

test("generates a distinct Idempotency-Key for every knowledge mutation action", async () => {
  renderWithClient(<MutationHarness />);
  fireEvent.click(screen.getByRole("button", { name: "idle" }));
  await screen.findByRole("button", { name: "done" });

  expect(knowledgeIdempotencyKeys).toHaveLength(6);
  expect(new Set(knowledgeIdempotencyKeys)).toHaveLength(6);
  for (const key of knowledgeIdempotencyKeys) expect(key).toMatch(/^[0-9a-f-]{36}$/i);
});

test("the default selected knowledge base is available for search", async () => {
  renderWithClient(<KnowledgePage />);
  expect(await screen.findByText(sampleKnowledgeBase.name)).toBeInTheDocument();
  expect(screen.getByLabelText("Search knowledge")).toBeEnabled();
});
