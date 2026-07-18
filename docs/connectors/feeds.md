# RSS and Atom connectors

`rss` and `atom` are separate providers and manifests. They share only provider-local polling and
sanitization helpers and expose no outbound actions.

## Setup and resources

Provide one public HTTP(S) feed URL through generic setup. Setup fetches the URL through
`ConnectorHttpClient`, follows at most three validated redirects, verifies the selected format, and
stores the canonical final URL as non-secret binding metadata. Resource refresh exposes the feed as
a selected provider resource.

Configure the provider's required trigger-session target before polling.

## Polling

Both manifests declare a 15-minute recurring sync cadence. Each selected feed resource is fetched
with the shared single-resolution, IP-pinned client:

- HTTP and HTTPS only;
- private, loopback, link-local, reserved, IPv4-mapped private, and RFC6598 addresses rejected;
- DNS/IP validation repeated for every redirect;
- 10-second timeout, three redirects, and a 1 MiB response limit;
- `If-None-Match` and `If-Modified-Since` sent from the resource cursor;
- `304 Not Modified` accepted without parsing or emitting events;
- conditional headers removed by the shared client on cross-origin redirects.

RSS and Atom use separate defensive stdlib XML parsers. DTD/entity declarations, malformed XML,
more than 5,000 elements, nesting deeper than 32 levels, and more than 200 items are rejected.
Titles and content are bounded, normalized to text, and stripped of HTML, scripts, and styles.

Canonical item identifiers prefer RSS `guid` or Atom `id`, then the item link, then a stable hash of
bounded item fields. A bounded cursor retains the most recent 256 identifiers for deduplication.
New items become tainted `rss.item` or `atom.item` durable events with feed URL/title and item
identifier, URL, publication time, content revision, binding, and connector provenance.
