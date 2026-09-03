#!/usr/bin/env bash
#
# validate_genie_sp.sh
# ---------------------
# Diagnose 403 / permission errors when calling the Databricks Genie API
# as a service principal (M2M OAuth), and validate the API end-to-end.
#
# It walks each layer in order and tells you the FIRST one that fails:
#
#   1. Token       - can the SP get an OAuth access token?         (authentication)
#   2. Identity    - who does that token actually resolve to?      (SCIM /Me)
#   3. Space       - can that identity READ the Genie space?       (authorization)
#   4. Conversation- can it START a conversation + get an answer?  (warehouse + table grants)
#
# A 403 almost never means "bad password" - it means the SP authenticated fine
# but is not AUTHORIZED for the space, the warehouse, or the underlying tables.
# This script pinpoints which one.
#
# ----------------------------------------------------------------------------
# AUTH NOTE (Azure vs AWS):
#   On AZURE Databricks, the workspace's own /oidc/v1/token endpoint IS backed by
#   Microsoft Entra ID, because Azure Databricks is a first-party Azure service.
#   So for an Entra-backed service principal:
#       CLIENT_ID     = the SP's Entra Application (client) ID
#       CLIENT_SECRET = a Databricks-managed OAuth secret on that SP
#                       (Workspace/Account > Identity > Service principals > Secrets)
#   This is the same call on AWS and Azure - only where the SP is defined differs.
#
#   If you already have a bearer token (e.g. an Entra AAD token from
#   `az account get-access-token`, or a PAT), set DATABRICKS_TOKEN and the
#   script skips step 1 and tests everything else with that token.
# ----------------------------------------------------------------------------
#
# USAGE:
#   1. Copy .env.example to .env and fill it in (or export the vars).
#   2. ./validate_genie_sp.sh
#
# REQUIREMENTS: bash + curl. (jq is used if present, but is NOT required.)

# Do not use `set -e`: we want to walk every layer and report, not bail early.
set -u

# ---- load .env if present ---------------------------------------------------
# Values already in the environment WIN; .env only fills in what is unset,
# so you can override any single value inline, e.g.  GENIE_SPACE_ID=xxx ./...
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"                       # strip CRLF
    case "$line" in ''|'#'*) continue ;; esac  # skip blanks/comments
    key="${line%%=*}"
    if [ -z "${!key:-}" ]; then export "$key=${line#*=}"; fi
  done < "$SCRIPT_DIR/.env"
fi

# ---- config (env vars) ------------------------------------------------------
HOST="${DATABRICKS_HOST:-}"
SPACE="${GENIE_SPACE_ID:-}"
CLIENT_ID="${CLIENT_ID:-}"
CLIENT_SECRET="${CLIENT_SECRET:-}"
TOKEN="${DATABRICKS_TOKEN:-}"
QUESTION="${GENIE_QUESTION:-What data is available in this space?}"

# ---- pretty output ----------------------------------------------------------
if [ -t 1 ]; then RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; BLD=$'\033[1m'; RST=$'\033[0m'
else RED=; GRN=; YEL=; BLD=; RST=; fi
pass() { printf "  ${GRN}PASS${RST} %s\n" "$1"; }
fail() { printf "  ${RED}FAIL${RST} %s\n" "$1"; }
info() { printf "       %s\n" "$1"; }
hdr()  { printf "\n${BLD}%s${RST}\n" "$1"; }
die()  { printf "\n${RED}== STOP: %s ==${RST}\n" "$1"; exit 1; }

# ---- tiny JSON string extractor (no jq needed) ------------------------------
# json_str <key>   reads stdin, prints first string value for "key":"..."
json_str() { grep -o "\"$1\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | head -1 | sed 's/.*:[[:space:]]*"//; s/"$//'; }

# normalize host (strip trailing slash)
HOST="${HOST%/}"

# ---- preflight --------------------------------------------------------------
hdr "Config"
[ -n "$HOST" ]  || die "DATABRICKS_HOST is not set (e.g. https://adb-1234.11.azuredatabricks.net)"
[ -n "$SPACE" ] || die "GENIE_SPACE_ID is not set"
info "host        : $HOST"
info "genie space : $SPACE"
if [ -n "$TOKEN" ]; then
  info "auth        : pre-supplied DATABRICKS_TOKEN (skipping OAuth step 1)"
else
  [ -n "$CLIENT_ID" ]     || die "CLIENT_ID is not set (and no DATABRICKS_TOKEN given)"
  [ -n "$CLIENT_SECRET" ] || die "CLIENT_SECRET is not set (and no DATABRICKS_TOKEN given)"
  info "auth        : OAuth M2M as client_id $CLIENT_ID"
fi

# =============================================================================
# STEP 1 - Token (authentication)
# =============================================================================
hdr "Step 1 - OAuth token   POST /oidc/v1/token"
if [ -n "$TOKEN" ]; then
  pass "using supplied DATABRICKS_TOKEN (OAuth step skipped)"
else
  RESP=$(curl -sS -w $'\n%{http_code}' -X POST "$HOST/oidc/v1/token" \
           -u "$CLIENT_ID:$CLIENT_SECRET" \
           -d "grant_type=client_credentials&scope=all-apis")
  CODE=$(printf '%s' "$RESP" | tail -n1)
  BODY=$(printf '%s' "$RESP" | sed '$d')
  if [ "$CODE" = "200" ]; then
    TOKEN=$(printf '%s' "$BODY" | json_str access_token)
    if [ -z "$TOKEN" ]; then die "token endpoint returned 200 but no access_token found"; fi
    pass "HTTP 200 - got access token (length ${#TOKEN})"
  else
    fail "HTTP $CODE"
    info "$(printf '%s' "$BODY" | head -c 400)"
    echo
    info "${YEL}Diagnosis:${RST} authentication failed BEFORE any Genie call. Common causes:"
    info "  401/unauthorized_client - wrong CLIENT_ID/CLIENT_SECRET, or secret expired/deleted."
    info "  404 / wrong host        - DATABRICKS_HOST wrong, or SP not added to THIS workspace."
    info "  On Azure: confirm the SP has a Databricks-managed OAuth secret (not just an Entra secret)."
    die "cannot obtain a token"
  fi
fi

AUTH=(-H "Authorization: Bearer $TOKEN")

# =============================================================================
# STEP 2 - Identity  (who is this token?)
# =============================================================================
hdr "Step 2 - Identity      GET /api/2.0/preview/scim/v2/Me"
RESP=$(curl -sS -w $'\n%{http_code}' "${AUTH[@]}" "$HOST/api/2.0/preview/scim/v2/Me")
CODE=$(printf '%s' "$RESP" | tail -n1)
BODY=$(printf '%s' "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
  WHO=$(printf '%s' "$BODY" | json_str displayName)
  WHOID=$(printf '%s' "$BODY" | json_str id)
  pass "HTTP 200 - token resolves to: ${WHO:-?} (id ${WHOID:-?})"
  info "Confirm this is the service principal you EXPECT to be calling Genie."
else
  fail "HTTP $CODE"
  info "$(printf '%s' "$BODY" | head -c 400)"
  die "token is not valid for this workspace (expired token, or wrong host)"
fi

# =============================================================================
# STEP 3 - Space read (authorization on the Genie space itself)
# =============================================================================
hdr "Step 3 - Space access  GET /api/2.0/genie/spaces/$SPACE"
RESP=$(curl -sS -w $'\n%{http_code}' "${AUTH[@]}" "$HOST/api/2.0/genie/spaces/$SPACE")
CODE=$(printf '%s' "$RESP" | tail -n1)
BODY=$(printf '%s' "$RESP" | sed '$d')
case "$CODE" in
  200)
    TITLE=$(printf '%s' "$BODY" | json_str title)
    pass "HTTP 200 - can read space: ${TITLE:-<no title>}"
    ;;
  403)
    fail "HTTP 403 - PERMISSION_DENIED reading the space"
    info "$(printf '%s' "$BODY" | head -c 500)"
    echo
    if printf '%s' "$BODY" | grep -qi "required scopes"; then
      info "${YEL}Diagnosis: OAuth SCOPE problem (not a grant problem).${RST}"
      info "  This token lacks the 'genie' scope. This is the classic error when a"
      info "  react/Databricks App forwards a USER token (on-behalf-of) without"
      info "  declaring the genie scope. Fixes:"
      info "    - App (OBO): add 'genie' to user_api_scopes in databricks.yml and redeploy."
      info "    - This SP script: request scope=all-apis (default here) - re-check CLIENT creds."
    else
      info "${YEL}Diagnosis: the identity above is NOT granted on this Genie space.${RST}"
      info "  Fix: grant it CAN_RUN on the space (Genie space > Share > add the SP)."
      info "  CLI: databricks api patch /api/2.0/permissions/genie/$SPACE \\"
      info "         --json '{\"access_control_list\":[{\"service_principal_name\":\"<client_id>\",\"permission_level\":\"CAN_RUN\"}]}'"
    fi
    die "not authorized on the Genie space"
    ;;
  404)
    fail "HTTP 404 - space not found"
    info "GENIE_SPACE_ID is wrong, or it lives in a DIFFERENT workspace than DATABRICKS_HOST."
    die "space not found on this host"
    ;;
  *)
    fail "HTTP $CODE"
    info "$(printf '%s' "$BODY" | head -c 500)"
    die "unexpected response reading the space"
    ;;
esac

# =============================================================================
# STEP 4 - Conversation (exercises warehouse + underlying table grants)
# =============================================================================
hdr "Step 4 - Conversation  POST /start-conversation  (question: \"$QUESTION\")"
RESP=$(curl -sS -w $'\n%{http_code}' -X POST \
         "$HOST/api/2.0/genie/spaces/$SPACE/start-conversation" \
         "${AUTH[@]}" -H "Content-Type: application/json" \
         -d "{\"content\":\"$QUESTION\"}")
CODE=$(printf '%s' "$RESP" | tail -n1)
BODY=$(printf '%s' "$RESP" | sed '$d')
if [ "$CODE" != "200" ]; then
  fail "HTTP $CODE"
  info "$(printf '%s' "$BODY" | head -c 500)"
  info "${YEL}You can READ the space but not start a conversation - likely CAN_VIEW"
  info "granted where CAN_RUN is required. Grant CAN_RUN on the space.${RST}"
  die "cannot start a conversation"
fi
CONV=$(printf '%s' "$BODY" | json_str conversation_id)
MSG=$(printf '%s' "$BODY" | json_str message_id)
pass "HTTP 200 - conversation started (conv $CONV / msg $MSG)"

info "polling for completion ..."
STATUS=""
for i in $(seq 1 30); do
  MB=$(curl -sS "${AUTH[@]}" "$HOST/api/2.0/genie/spaces/$SPACE/conversations/$CONV/messages/$MSG")
  STATUS=$(printf '%s' "$MB" | json_str status)
  case "$STATUS" in
    COMPLETED|FAILED|CANCELLED|QUERY_RESULT_EXPIRED) break ;;
  esac
  sleep 3
done

if [ "$STATUS" = "COMPLETED" ]; then
  pass "message COMPLETED - Chat mode answered end-to-end"
  # show the answer text if we can find it
  ANS=$(printf '%s' "$MB" | grep -o '"content"[[:space:]]*:[[:space:]]*"[^"]*"' | tail -1 | sed 's/.*:[[:space:]]*"//; s/"$//')
  [ -n "$ANS" ] && info "answer: $(printf '%s' "$ANS" | head -c 300)"
else
  fail "message ended in status: ${STATUS:-<unknown/timeout>}"
  info "$(printf '%s' "$MB" | head -c 600)"
  info "${YEL}Chat mode started but query execution did not complete.${RST}"
  info "  Usually table/warehouse grants: grant the SP CAN_USE on the warehouse and"
  info "  SELECT on the UC tables (+ USE CATALOG / USE SCHEMA). Same grants feed"
  info "  Agent mode below, so fix this and both modes benefit."
fi

# =============================================================================
# STEP 5 - Agent mode (PREVIEW)   POST /api/2.0/genie/agents/{id}/responses
# =============================================================================
# The newer streaming (SSE), Responses-API-shaped surface a React app would use.
# NOTE: agent_id == the Genie space id (spaces are surfaced as "Genie Agents").
# It is a PREVIEW: a workspace that is not enrolled / has the toggle off fails
# HERE even when Chat mode (above) works. If your app calls THIS endpoint, this
# is the step whose result matters to you.
hdr "Step 5 - Agent mode    POST /api/2.0/genie/agents/$SPACE/responses  (preview, SSE)"
AFILE="$(mktemp)"
ACODE=$(curl -sS -N --max-time 45 -o "$AFILE" -w '%{http_code}' -X POST \
          "$HOST/api/2.0/genie/agents/$SPACE/responses" \
          "${AUTH[@]}" -H "Content-Type: application/json" -H "Accept: text/event-stream" \
          -d "{\"input\":[{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"$QUESTION\"}]}]}")
ABODY=$(head -c 800 "$AFILE"); rm -f "$AFILE"
case "$ACODE" in
  200)
    if printf '%s' "$ABODY" | grep -q "response.created"; then
      pass "HTTP 200 - Agent mode streaming works (SSE 'response.created' received)"
    else
      pass "HTTP 200 - Agent mode responded"
    fi
    ;;
  400|403|404)
    fail "HTTP $ACODE"
    info "$(printf '%s' "$ABODY" | head -c 400)"
    if printf '%s' "$ABODY" | grep -qiE "preview|enroll|not enabled|toggle"; then
      info "${YEL}Diagnosis: Agent mode is a PREVIEW that is NOT enabled in this workspace.${RST}"
      info "  Chat mode can pass while this fails. Enable the Genie Agent mode / Responses"
      info "  preview for the workspace, or use Chat mode (steps 3-4) instead."
    elif printf '%s' "$ABODY" | grep -qi "permission"; then
      info "${YEL}Diagnosis: same grant story as Chat mode - grant the SP CAN_RUN on the space.${RST}"
    else
      info "${YEL}If your code calls THIS endpoint, this is the 403 to chase"
      info "(preview enrollment first, then the space grant).${RST}"
    fi
    ;;
  *)
    fail "HTTP $ACODE"
    info "$(printf '%s' "$ABODY" | head -c 400)"
    ;;
esac

echo
printf "${BLD}Done. The FIRST failing step above is your root cause"
printf " (Chat mode = steps 3-4, Agent mode = step 5).${RST}\n"
