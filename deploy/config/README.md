# Mounted configuration

Files here are mounted into services (see `docker-compose.yml`) as the
file-based config layer (DR-3). Precedence: defaults → these files → `KEEL_*`
env → runtime overrides (see `keel_core.config`).

Empty in M0; layered file sources are wired when config grows in M1.
