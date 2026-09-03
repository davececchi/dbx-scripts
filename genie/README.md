# Genie API service-principal validator

A single bash script (`validate_genie_sp.sh`) that diagnoses **403 /
permission errors** when calling the Databricks **Genie API** as a service
principal, and validates the space end-to-end. Only `bash` + `curl` are
required.

## Why a 403 is (almost) never a password problem

The Genie API sits behind four independent layers. A 403 means the caller
authenticated fine but is not **authorized** at one of them. The script walks
them in order and stops at the first failure, so you know exactly what to fix:

| Step | Call | A failure here means |
|------|------|----------------------|
| 1. Token | `POST /oidc/v1/token` | Bad `CLIENT_ID`/`CLIENT_SECRET`, expired secret, wrong host, or SP not in this workspace. |
| 2. Identity | `GET /api/2.0/preview/scim/v2/Me` | Token invalid/expired, or points at the wrong identity. |
| 3. Space | `GET /api/2.0/genie/spaces/{id}` | SP not granted **CAN_RUN** on the Genie space (the classic 403), or missing the `genie` OAuth scope. |
| 4. Conversation (Chat mode) | `POST /api/2.0/genie/spaces/{id}/start-conversation` + poll | SP lacks **CAN_USE** on the SQL warehouse, or **SELECT** on the underlying Unity Catalog tables. |
| 5. Agent mode (preview) | `POST /api/2.0/genie/agents/{id}/responses` (SSE) | Agent-mode **preview not enabled** in the workspace (Chat mode can still pass), or the same space grant is missing. |

## Chat mode vs Agent mode — target the one your code calls

There are two API surfaces onto the same Genie space, and the script tests
both:

- **Chat mode** — `POST /api/2.0/genie/spaces/{id}/start-conversation`, then
  poll messages. Stateful, poll-based. GA. (This is what the AppKit `<GenieChat>`
  plugin uses under the hood today.)
- **Agent mode** — `POST /api/2.0/genie/agents/{id}/responses`. An OpenAI
  Responses-API-shaped, **SSE-streaming** endpoint that streams reasoning → SQL
  → answer with citations. **Preview.** `agent_id` is the **same id** as the
  Genie space. This is the better fit for a streaming React chat UI.

If generated code is hitting `/agents/{id}/responses` and getting a 403, note
that Agent mode is a **preview**: a workspace that isn't enrolled fails at
step 5 even when Chat mode (steps 3–4) passes. Enable the preview first, then
check the space grant.

## Usage

```bash
cp .env.example .env      # then fill it in
./validate_genie_sp.sh
```

Override any single value inline, e.g.:

```bash
GENIE_SPACE_ID=<other-space> ./validate_genie_sp.sh
DATABRICKS_TOKEN=<aad-or-pat> ./validate_genie_sp.sh   # skip OAuth, test a token
```

## Fixing the common failures

- **Step 3, 403, "not granted":** grant the SP CAN_RUN on the space.
  ```bash
  databricks api patch /api/2.0/permissions/genie/<SPACE_ID> \
    --json '{"access_control_list":[{"service_principal_name":"<client_id>","permission_level":"CAN_RUN"}]}'
  ```
- **Step 3, 403, "required scopes: genie":** you're on the OBO path — add
  `genie` to `user_api_scopes` in `databricks.yml` and redeploy the app.
- **Step 4, didn't complete:** grant the SP CAN_USE on the warehouse and
  SELECT on the tables (plus USE CATALOG / USE SCHEMA).
