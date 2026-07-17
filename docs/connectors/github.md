# GitHub connector

The `github` connector is a GitHub App collaboration connector. It is intentionally separate
from managed code projects: it does not clone repositories, create branches, mutate code, merge
pull requests, run sandboxes, or import/synchronize managed projects.

## GitHub App configuration

Create a GitHub App with:

- repository permissions: **Metadata: read**, **Issues: read/write**,
  **Pull requests: read**, and **Commit statuses: read**;
- Issue, pull request, and issue-comment webhook events;
- the connector callback URL as the App setup URL;
- the connector webhook URL as the webhook URL.

Stage setup by saving:

- App ID, client ID, and App slug;
- private-key and webhook-secret references;
- optional GitHub API/web base URLs for GitHub Enterprise.

Secret references are `env:NAME` or `file:ABSOLUTE_PATH`. Keel resolves the App private key only
when signing an App JWT and never persists its plaintext. Installation access tokens are minted
just in time, narrowed to the selected repository, held only in process memory, and discarded.
The durable connector credential contains only App configuration and secret references.

After setup, choose **Install GitHub App**, complete the browser installation, refresh repository
resources, and select the repositories the connector may use. Every read and write rechecks that
selection.

## Tools and safety

Read tools expose selected repository metadata, Issues, pull requests, conversation comments, and
commit status. Results carry GitHub provenance and are tainted external content. Pagination is
bounded, and rate-limit, permission, revoked-installation, and missing-repository access failures
are explicit.

Outbound tools are limited to:

- creating an Issue;
- creating an Issue or pull-request conversation comment.

Both require an idempotency key and tainted-content approval. A hashed idempotency marker is
embedded in the created body so a retry can reconcile a provider-side success even if the first
response was lost. Comment retries query a bounded seven-day update window and inspect the newest
pagination tail, so large historical comment threads do not hide a recently accepted marker.

Webhooks require `X-Hub-Signature-256`. `X-GitHub-Delivery` is durably replay-protected by the
connector foundation; only selected-repository Issue, pull-request, and issue-comment events are
admitted.

Health verifies that the installation still exists, is not suspended, retains required
permissions, and can access every selected repository. Uninstall, permission shrink, repository
removal, and local revoke all fail closed.
