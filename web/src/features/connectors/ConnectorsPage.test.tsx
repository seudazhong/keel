import { fireEvent, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, expect, test, vi } from "vitest";
import { emptyAuth, setAuthSnapshot } from "../auth/authState";
import { makeConnectorFixture } from "../../test/connectorFixtures";
import { server } from "../../test/setup";
import { renderWithClient } from "../../test/utils";
import { ConnectorsPage } from "./ConnectorsPage";

afterEach(() => {
  setAuthSnapshot(emptyAuth);
  vi.restoreAllMocks();
});

test("renders a connected manifest with scope chips and taint guidance", async () => {
  renderWithClient(<ConnectorsPage />);
  expect(await screen.findByText("gmail.readonly")).toBeInTheDocument();
  expect(screen.getByText("gmail.send")).toBeInTheDocument();
  expect(screen.getByText(/External connector content is tainted/)).toBeInTheDocument();
  expect(screen.getByText("healthy")).toBeInTheDocument();
});

test("a not-connected oauth connector requests an authenticated connect URL and opens it", async () => {
  let connectRequest: Request | undefined;
  let legacyConnectGetUsed = false;
  setAuthSnapshot({
    credential: { kind: "api-key", secret: "connector-key" },
    org: "test-org",
    agent: "test-agent",
  });
  server.use(
    http.get("/v1/connectors/:id/connect", () => {
      legacyConnectGetUsed = true;
      return HttpResponse.json({ detail: "Legacy route must not be used" }, { status: 500 });
    }),
    http.post("/v1/connectors/:id/connect-url", ({ request }) => {
      connectRequest = request;
      return HttpResponse.json({ url: "https://provider.example/consent" });
    }),
  );
  const open = vi.spyOn(window, "open").mockReturnValue({} as Window);
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("gmail.readonly");
  expect(screen.getByText("OAuth fixture")).toBeInTheDocument();
  const btn = screen.getByRole("button", { name: "Connect" });
  expect(btn).not.toBeDisabled();
  fireEvent.click(btn);
  await waitFor(() =>
    expect(open).toHaveBeenCalledWith(
      "https://provider.example/consent",
      "_blank",
      "noopener,noreferrer",
    ),
  );
  expect(connectRequest?.method).toBe("POST");
  expect(new URL(connectRequest!.url).pathname).toBe("/v1/connectors/oauth-fixture/connect-url");
  expect(connectRequest?.headers.get("X-API-Key")).toBe("connector-key");
  expect(connectRequest?.headers.get("X-Keel-Org")).toBe("test-org");
  expect(connectRequest?.headers.get("X-Keel-Agent")).toBe("test-agent");
  expect(legacyConnectGetUsed).toBe(false);
});

test("a blocked connector authorization popup shows an actionable error", async () => {
  vi.spyOn(window, "open").mockReturnValue(null);
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("OAuth fixture");
  fireEvent.click(screen.getByRole("button", { name: "Connect" }));
  expect(
    await screen.findByText(/authorization window was blocked.*Allow popups.*try again/i),
  ).toBeInTheDocument();
});

test("configured staged connector keeps setup and authorization controls visible", async () => {
  server.use(
    http.get("/v1/connectors", () =>
      HttpResponse.json([
        makeConnectorFixture({
          id: "staged",
          name: "Staged",
          auth_kind: "app_credentials",
          configured: true,
          connected: false,
          auth_action: {
            label: "Authorize staged app",
            callback_parameters: [],
            requires_setup: true,
            help_text: "Save app credentials, then authorize in the browser.",
          },
          next_action: {
            kind: "authorize",
            label: "Authorize staged app",
            instructions: "Save app credentials, then authorize in the browser.",
          },
          binding: {
            id: "staged-binding",
            status: "configured",
            display_name: "Staged",
            external_account_id: null,
            external_tenant_id: null,
            metadata: {},
            last_success_at: null,
            error_code: null,
            error_summary: null,
            renewal_expires_at: null,
            next_sync_at: null,
            next_renewal_at: null,
            targets: {},
          },
        }),
      ]),
    ),
  );
  renderWithClient(<ConnectorsPage />);
  expect(
    await screen.findByText("Save app credentials, then authorize in the browser."),
  ).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Authorize staged app" })).not.toBeDisabled();
  expect(screen.getByLabelText("Client secret")).toBeInTheDocument();
});

test("revoking a connector removes it from the connected list", async () => {
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("gmail.readonly");
  fireEvent.click(screen.getByRole("button", { name: "Disconnect" }));
  await waitFor(() => expect(screen.queryByText("gmail.readonly")).not.toBeInTheDocument());
});

test("an unavailable provider exposes explicit local-forget controls", async () => {
  let requested = "";
  const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
  server.use(
    http.get("/v1/connectors", () =>
      HttpResponse.json([
        makeConnectorFixture({
          id: "broken",
          name: "Broken",
          auth_kind: "secret",
          connected: true,
          available: false,
          availability_error: "Missing optional dependency",
        }),
      ]),
    ),
    http.delete("/v1/connectors/broken/purge/local", () => {
      requested = "forced";
      return HttpResponse.json({ ok: true });
    }),
  );
  renderWithClient(<ConnectorsPage />);
  expect(await screen.findByText(/Missing optional dependency/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Force local purge" }));
  await waitFor(() => expect(requested).toBe("forced"));
  confirm.mockRestore();
});

test("secret setup never echoes the entered value", async () => {
  renderWithClient(<ConnectorsPage />);
  const input = await screen.findByLabelText("Secret");
  fireEvent.change(input, { target: { value: "top-secret" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await waitFor(() => expect(input).toHaveValue(""));
  expect(screen.queryByText("top-secret")).not.toBeInTheDocument();
});

test("generated setup secrets are rendered outside React state and can be hidden", async () => {
  server.use(
    http.post("/v1/connectors/:id/setup", () =>
      HttpResponse.json({
        ok: true,
        binding_id: "secret-binding",
        artifacts: [
          {
            kind: "secret",
            label: "Webhook secret",
            value: "generated-once",
            secret: true,
          },
        ],
      }),
    ),
  );
  renderWithClient(<ConnectorsPage />);
  fireEvent.change(await screen.findByLabelText("Secret"), {
    target: { value: "submitted-secret" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  expect(await screen.findByText("generated-once")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Hide secret values" }));
  expect(screen.queryByText("generated-once")).not.toBeInTheDocument();
});

test("renders and saves manifest-declared connector targets", async () => {
  let requestBody: unknown;
  server.use(
    http.get("/v1/connectors", () =>
      HttpResponse.json([
        makeConnectorFixture({
          id: "drive",
          name: "Drive",
          auth_kind: "oauth",
          connected: true,
          target_fields: [
            {
              kind: "knowledge",
              label: "Knowledge Base ID",
              required: true,
              help_text: "Choose a base in this scope.",
            },
          ],
        }),
      ]),
    ),
    http.put("/v1/connectors/drive/targets", async ({ request }) => {
      requestBody = await request.json();
      return HttpResponse.json({ ok: true, targets: { knowledge: "kb_123" } });
    }),
  );
  renderWithClient(<ConnectorsPage />);
  const target = await screen.findByLabelText("Knowledge Base ID");
  fireEvent.change(target, { target: { value: "kb_123" } });
  fireEvent.click(screen.getByRole("button", { name: "Save targets" }));
  await waitFor(() =>
    expect(requestBody).toEqual({ targets: { knowledge: "kb_123" } }),
  );
});
