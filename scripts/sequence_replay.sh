#!/usr/bin/env bash
# Acceptance replay for the field operation sequence integrity data product.
#
# Covers the six required situations and the product's hard rules:
#
#   1. reseeding (allowed alternative) and two fertilizer batches (repeat)
#   2. harvest earlier than the provable seeding (order conflict, both engines)
#   3. offline backfill: early event arriving late stays valid (event time)
#   4. boundary revision: a correction opens a new run, old runs still replay
#   5. overlapping boundaries and mutually exclusive crops stay conflicts
#   6. a retired crop-specific rule version leaves the season not-applicable
#
# plus the residency boundary: the group only gets region-level stage counts,
# never a field-level trajectory, and conflicts are never auto-resolved to
# improve the completion rate.
#
# Usage: scripts/sequence_replay.sh [BASE_URL]   (default http://127.0.0.1:8080)
set -uo pipefail

BASE="${1:-http://127.0.0.1:8080}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
S="$ROOT/samples"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

TOK_ADMIN="demo-admin-token"
TOK_ANALYST="demo-analyst-token"
TOK_AUDITOR="demo-auditor-token"

PASS=0
FAIL=0
CHECKS=()

api() {
  local method="$1" path="$2" token="$3" body="${4:-}"
  local args=(-sS -X "$method" "$BASE$path" -H "Authorization: Bearer $token")
  if [[ -n "$body" ]]; then
    if [[ "$body" == @* ]]; then
      args+=(-H 'Content-Type: application/json' --data-binary "@${body#@}")
    else
      args+=(-H 'Content-Type: application/json' --data-binary "$body")
    fi
  fi
  curl "${args[@]}"
}

check() {
  local name="$1" actual="$2" expected="$3"
  if [[ "$actual" == "$expected" ]]; then
    PASS=$((PASS + 1))
    printf '  \033[32mPASS\033[0m %-64s %s\n' "$name" "$actual"
    CHECKS+=("PASS|$name|$actual")
  else
    FAIL=$((FAIL + 1))
    printf '  \033[31mFAIL\033[0m %-64s got=%s want=%s\n' "$name" "$actual" "$expected"
    CHECKS+=("FAIL|$name|got=$actual want=$expected")
  fi
}

contains() {
  local name="$1" hay="$2" needle="$3"
  if [[ "$hay" == *"$needle"* ]]; then
    PASS=$((PASS + 1))
    printf '  \033[32mPASS\033[0m %-64s contains %s\n' "$name" "$needle"
    CHECKS+=("PASS|$name|contains $needle")
  else
    FAIL=$((FAIL + 1))
    printf '  \033[31mFAIL\033[0m %-64s missing %s\n' "$name" "$needle"
    CHECKS+=("FAIL|$name|missing $needle")
  fi
}

section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

sql_control() {
  PGPASSWORD="${PGPASSWORD:-ccdev}" "${PSQL:-/opt/postgresql-18/bin/psql}" \
    -h "${PGHOST:-127.0.0.1}" -p "${PGPORT:-5432}" -U "${PGUSER:-ccdev}" \
    -d agrisync_control -tAc "$1" 2>/dev/null
}

OP_BR="demo-operator-br-mt"
OP_DE="demo-operator-de-bb"
REVIEW_BODY='{"period_from":"2026-11-01T00:00:00Z","period_to":"2026-11-30T00:00:00Z"}'

echo "AgriSync sequence-integrity acceptance replay against $BASE"

section "0. authentication and the field-level boundary"
check "no token is rejected" \
  "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/regions/BR-MT/sequence/reviews")" "401"
check "group analyst has no path to field-level reviews" \
  "$(api GET /regions/BR-MT/sequence/reviews "$TOK_ANALYST" | jq -r .error.code)" "forbidden"
check "group analyst has no path to sequence facts" \
  "$(api GET /regions/BR-MT/sequence/facts "$TOK_ANALYST" | jq -r .error.code)" "forbidden"
check "region operator is scoped to one region" \
  "$(api GET /regions/DE-BB/sequence/reviews "$OP_BR" | jq -r .error.code)" "forbidden"
check "analyst may read the count-only coverage feed" \
  "$(api GET /sequence/coverage/publications "$TOK_ANALYST" | jq -r 'has("publications")')" "true"
check "operator cannot build a group rollup" \
  "$(api POST /sequence/coverage/rollups "$OP_BR" '{"engine":"fsm","period_from":"2026-11-01T00:00:00Z","period_to":"2026-11-30T00:00:00Z","scope_regions":["BR-MT"]}' | jq -r .error.code)" "forbidden"

section "0b. reset and load standardized inputs"
api POST /demo/reset "$TOK_ADMIN" >/dev/null
for f in seasons boundaries calendars rules facts; do
  out=$(api POST "/regions/BR-MT/sequence/$f" "$OP_BR" "@$S/sequence/br-mt-$f.json")
  check "load $f" "$(jq -r '.stored // .written' <<<"$out" | head -1)" "$(jq -r '.seasons // .boundaries // .calendars // .rules // .facts | length' "$S/sequence/br-mt-$f.json")"
done
check "default FSM rule set seeded" \
  "$(api GET /regions/BR-MT/sequence/rules "$OP_BR" | jq -r '[.rule_sets[]|select(.engine=="fsm" and .status=="active")]|length')" "1"
check "default partial-order rule set seeded" \
  "$(api GET /regions/BR-MT/sequence/rules "$OP_BR" | jq -r '[.rule_sets[]|select(.engine=="partial_order" and .status=="active")]|length')" "1"
check "sorghum FSM rule version is retired" \
  "$(api GET /regions/BR-MT/sequence/rules "$OP_BR" | jq -r '.rule_sets[]|select(.crop_code=="sorghum")|.status')" "retired"

section "1. review run: reseeding, repeats, conflicts and pending scopes"
RUN1=$(api POST /regions/BR-MT/sequence/reviews "$OP_BR" "$REVIEW_BODY")
echo "$RUN1" > "$TMP/run1.json"
RID1=$(jq -r .run.run_id "$TMP/run1.json")
check "review run created" "$(jq -r .run.status "$TMP/run1.json")" "completed"
check "run freezes a fixed version bundle" \
  "$(jq -r '.run.version_bundle.rule_sets|keys|sort|join(",")' "$TMP/run1.json")" "fsm,partial_order"
check "run records a fact version" "$(jq -r '.run.fact_version|startswith("fv_")' "$TMP/run1.json")" "true"
check "run records an attribution version" "$(jq -r '.run.attribution_version|startswith("av_")' "$TMP/run1.json")" "true"
check "both engines review all seven field seasons" \
  "$(jq -r '.engines[]|.field_count' "$TMP/run1.json" | sort -u | tr '\n' ',')" "7,"
check "both engines surface conflicts rather than hiding them" \
  "$(jq -r '.engines[]|.status' "$TMP/run1.json" | sort -u | tr '\n' ',')" "conflict,"

# MT-F-11: clean explainable sequence with alternative + repeat.
check "F11 FSM completes all stages" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f11-2026-maize")|.fsm_status' "$TMP/run1.json")" "ok"
check "F11 partial-order completes all stages" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f11-2026-maize")|.po_status' "$TMP/run1.json")" "ok"
check "F11 FSM ends in harvested" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f11-2026-maize")|.fsm_state' "$TMP/run1.json")" "harvested"
check "F11 reseeding is an allowed alternative" \
  "$(jq -r '.events[]|select(.fact_id=="fact-f11-seed-2" and .engine=="fsm")|.interpretation' "$TMP/run1.json")" "alternative"
check "F11 second fertilizer is an allowed repeat" \
  "$(jq -r '.events[]|select(.fact_id=="fact-f11-fert-2" and .engine=="fsm")|.interpretation' "$TMP/run1.json")" "duplicate"
check "F11 partial-order treats reseeding as alternative too" \
  "$(jq -r '.events[]|select(.fact_id=="fact-f11-seed-2" and .engine=="partial_order")|.interpretation' "$TMP/run1.json")" "alternative"
check "F11 partial-order keeps several feasible stage paths" \
  "$(jq -r '[.fields[]|select(.season_id=="mt-f11-2026-maize")|.allowed_paths[]|select(.stage_path!=null)]|length' "$TMP/run1.json")" "2"

# MT-F-12: harvest before provable seeding.
check "F12 FSM order conflict" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f12-2026-soy")|.fsm_status' "$TMP/run1.json")" "conflict"
check "F12 partial-order order conflict" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f12-2026-soy")|.po_status' "$TMP/run1.json")" "conflict"
check "F12 conflict evidence is recorded by both engines" \
  "$(jq -r '[.issues[]|select(.season_id=="mt-f12-2026-soy" and .code=="order_conflict")]|length' "$TMP/run1.json")" "2"
check "F12 later seeding stays confirmed (late facts never negate evidence)" \
  "$(jq -r '.events[]|select(.fact_id=="fact-f12-seed-1" and .engine=="fsm")|.interpretation' "$TMP/run1.json")" "confirmed"
check "F12 early harvest keeps its conflict interpretation" \
  "$(jq -r '.events[]|select(.fact_id=="fact-f12-harv-1" and .engine=="partial_order")|.interpretation' "$TMP/run1.json")" "conflict"

# MT-F-13: offline late arrival uses event time.
check "F13 offline backfill still validates (FSM)" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f13-2026-cotton")|.fsm_status' "$TMP/run1.json")" "ok"
check "F13 backfilled seed is flagged late and confirmed" \
  "$(jq -r '.events[]|select(.fact_id=="fact-f13-seed-1" and .engine=="fsm")|"\(.interpretation):\(.detail.late_arrival)"' "$TMP/run1.json")" "confirmed:true"
check "F13 events are ordered by event time not arrival order" \
  "$(jq -r '[.events[]|select(.season_id=="mt-f13-2026-cotton" and .engine=="fsm")]|.[0].fact_id' "$TMP/run1.json")" "fact-f13-seed-1"

# MT-F-21: incomplete evidence stays pending with an explicit scope.
check "F21 stays pending while harvest evidence is missing" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f21-2026-maize")|.fsm_status' "$TMP/run1.json")" "pending_review"
check "F21 names the missing harvested prerequisite" \
  "$(jq -r '[.fields[]|select(.season_id=="mt-f21-2026-maize")|.missing_prerequisites[]|select(.stage=="harvested")]|length' "$TMP/run1.json")" "2"
check "F21 pending ranges cover the season window for the gap" \
  "$(jq -r '[.fields[]|select(.season_id=="mt-f21-2026-maize")|.pending_ranges[]|select(.stage=="harvested")]|length' "$TMP/run1.json")" "2"
check "F21 corrected boundary is not mistaken for an overlap" \
  "$(jq -r '[.issues[]|select(.season_id=="mt-f21-2026-maize" and .code=="boundary_overlap")]|length' "$TMP/run1.json")" "0"

# MT-F-22 / MT-F-23: attribution conflicts cannot be resolved by picking a path.
check "F22 overlapping boundaries keep the season conflicted" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f22-2026-maize")|.attribution_status' "$TMP/run1.json")" "conflict"
check "F22 boundary overlap evidence exists for both engines" \
  "$(jq -r '[.issues[]|select(.season_id=="mt-f22-2026-maize" and .code=="boundary_overlap")]|length' "$TMP/run1.json")" "2"
check "F23 mutually exclusive crops keep the season conflicted" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f23-2026-mix")|.attribution_status' "$TMP/run1.json")" "conflict"
check "F23 mutex crop evidence names both crops" \
  "$(jq -r '[.issues[]|select(.season_id=="mt-f23-2026-mix" and .code=="mutex_crop_same_window")][0].evidence.crops|sort|join(",")' "$TMP/run1.json")" "maize,soybean"

# MT-F-24: rule version not applicable to the variety.
check "F24 rule set is not applicable to sorghum" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f24-2026-sorghum")|.rule_applicability' "$TMP/run1.json")" "not_applicable"
check "F24 stays pending under both engines" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f24-2026-sorghum")|"\(.fsm_status):\(.po_status)"' "$TMP/run1.json")" "pending_review:pending_review"
check "F24 not-applicable reason is recorded for both engines" \
  "$(jq -r '[.issues[]|select(.season_id=="mt-f24-2026-sorghum" and .code=="rule_version_not_applicable")]|length' "$TMP/run1.json")" "2"

section "2. corrections open a new run; the old summary still replays"
api POST /regions/BR-MT/sequence/facts "$OP_BR" '{"facts":[{"fact_id":"fact-f21-harv-1","device_serial":"BR-HARV-2102","segment_id":"seq-f21-h1","operation_type":"harvesting","field_id":"MT-F-21","event_at":"2026-11-17T10:00:00Z"}]}' >/dev/null
RUN2=$(api POST /regions/BR-MT/sequence/reviews "$OP_BR" '{"period_from":"2026-11-01T00:00:00Z","period_to":"2026-11-30T00:00:00Z","note":"after correction"}')
RID2=$(jq -r .run.run_id <<<"$RUN2")
check "correction produced a new run" "$([[ "$RID2" != "$RID1" ]] && echo yes)" "yes"
check "new run has a new fact version" \
  "$([[ "$(jq -r .run.fact_version <<<"$RUN2")" != "$(jq -r .run.fact_version "$TMP/run1.json")" ]] && echo changed)" "changed"
check "F21 is complete in the new run" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f21-2026-maize")|.fsm_status' <<<"$RUN2")" "ok"
OLD=$(api GET "/regions/BR-MT/sequence/reviews/$RID1" "$OP_BR")
check "old run still replays its original pending verdict" \
  "$(jq -r '.fields[]|select(.season_id=="mt-f21-2026-maize")|.fsm_status' <<<"$OLD")" "pending_review"
check "both runs remain listable" \
  "$(api GET /regions/BR-MT/sequence/reviews "$OP_BR" | jq -r '.runs|length')" "2"

section "3. only approved-granularity coverage leaves the region"
PUB=$(api POST "/regions/BR-MT/sequence/reviews/$RID2/publish" "$OP_BR")
echo "$PUB" > "$TMP/pub.json"
check "publication covers both engines" "$(jq -r '.publications|length' "$TMP/pub.json")" "2"
check "publication declares the coarse granularity" "$(jq -r .granularity "$TMP/pub.json")" "region_stage_counts"
check "publication payload carries no field id" \
  "$(grep -c 'MT-F' "$TMP/pub.json" || true)" "0"
check "FSM stage counts match the in-region rollup" \
  "$(jq -r '.publications[]|select(.engine=="fsm")|.stage_counts|to_entries|map("\(.key)=\(.value)")|sort|join(",")' "$TMP/pub.json")" \
  "fertilized=2,harvested=3,seeded=6"
check "control store holds no field id in the coverage table" \
  "$(sql_control "select count(*) from sequence_coverage_publications where stage_counts::text like '%MT-F%' or version_bundle::text like '%MT-F%'")" "0"

ROLL=$(api POST /sequence/coverage/rollups "$TOK_ANALYST" '{"engine":"fsm","period_from":"2026-11-01T00:00:00Z","period_to":"2026-12-31T00:00:00Z","scope_regions":["BR-MT","DE-BB"]}')
echo "$ROLL" > "$TMP/roll.json"
check "rollup includes the published region" "$(jq -r '.included_regions|join(",")' "$TMP/roll.json")" "BR-MT"
check "rollup names the region without coverage" "$(jq -r '.excluded_regions|join(",")' "$TMP/roll.json")" "DE-BB"
check "excluded region is explained, not counted as zero" \
  "$(jq -r '.region_breakdown[]|select(.region_code=="DE-BB")|.exclusion_reason' "$TMP/roll.json")" "no_confirmed_coverage_publication"
check "group stage totals equal the released counts" \
  "$(jq -r '.stage_totals|to_entries|map("\(.key)=\(.value)")|sort|join(",")' "$TMP/roll.json")" "fertilized=2,harvested=3,seeded=6"
check "group rollup contains no field identifier" \
  "$(grep -c 'MT-F' "$TMP/roll.json" || true)" "0"
check "control rollup table stores no field identifier" \
  "$(sql_control "select count(*) from sequence_coverage_rollups where region_breakdown::text like '%MT-F%'")" "0"
check "auditor can read the group coverage feed" \
  "$(api GET /sequence/coverage/publications "$TOK_AUDITOR" | jq -r '.publications|length')" "2"

section "4. uploaded operations can be standardized into facts"
api POST /regions/BR-MT/ingest "$OP_BR" "@$S/regions/br-mt.json" >/dev/null
check "operation events project into sequence facts" \
  "$(api POST /regions/BR-MT/sequence/facts/import "$OP_BR" | jq -r .imported)" "4"
check "projection is idempotent" \
  "$(api POST /regions/BR-MT/sequence/facts/import "$OP_BR" | jq -r .imported)" "0"
check "projected facts keep their uploaded provenance" \
  "$(api GET '/regions/BR-MT/sequence/facts?field_id=MT-F-11&limit=10' "$OP_BR" | jq -r '[.facts[]|select(.source=="uploaded")]|length')" "2"

section "summary"
printf 'checks passed: %d, failed: %d\n' "$PASS" "$FAIL"
if (( FAIL > 0 )); then
  printf '\n\033[31mFAILED CHECKS\033[0m\n'
  for c in "${CHECKS[@]}"; do
    [[ "$c" == FAIL* ]] && printf '  %s\n' "$c"
  done
  exit 1
fi
printf '\n\033[32mALL SEQUENCE CHECKS PASSED\033[0m\n'
