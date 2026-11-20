#!/usr/bin/env python3
"""Generate deterministic sample payloads for the field operation sequence
integrity product.

The six acceptance situations live in BR-MT across separate field seasons:

  MT-F-11  reseeding (alternative) + two fertilizer batches (repeat)
  MT-F-12  harvest earlier than the provable seeding (order conflict)
  MT-F-13  offline backfill: an early event arrives late but stays valid
  MT-F-21  boundary revision (a correction, not an overlap)
  MT-F-22  genuinely overlapping boundaries (attribution conflict)
  MT-F-23  mutually exclusive crops in the same window (attribution conflict)
  MT-F-24  sorghum season whose crop-specific rule version is retired
           (rule version not applicable to the variety)

All timestamps are UTC. event_at is when the work happened; received_at is
when the region learned about it, so the offline facts have an early
event_at with a late received_at.

Usage: python3 scripts/gen_sequence_samples.py
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "samples", "sequence")


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fact(fid, device, seg, op, field, event, received=None, completed=True):
    return {
        "fact_id": fid,
        "device_serial": device,
        "segment_id": seg,
        "operation_type": op,
        "field_id": field,
        "event_at": iso(event),
        "received_at": iso(received or event),
        "completed": completed,
        "batch_id": "sequence-demo",
        "detail": {},
    }


def season(sid, field, crop, frm, to, status="active", note=""):
    return {
        "season_id": sid, "field_id": field, "crop_code": crop,
        "planned_from": iso(frm), "planned_to": iso(to), "status": status, "note": note,
    }


def boundary(rid, field, frm, to=None, label="", superseded="", note=""):
    return {
        "revision_id": rid, "field_id": field, "valid_from": iso(frm),
        "valid_until": iso(to) if to else None, "boundary_label": label or rid,
        "geometry": {"type": "boundary_ref", "label": label or rid},
        "superseded_by": superseded, "note": note,
    }


def calendar(cid, field, sid, crop, frm, to=None, note=""):
    return {
        "calendar_id": cid, "field_id": field, "season_id": sid, "crop_code": crop,
        "valid_from": iso(frm), "valid_until": iso(to) if to else None, "note": note,
    }


def write(name, payload):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    print("wrote", os.path.relpath(os.path.join(OUT, name), ROOT))


def main():
    from datetime import datetime, timezone, timedelta
    UTC = timezone.utc

    def t(day, hour=8):
        return datetime(2026, 11, day, hour, 0, 0, tzinfo=UTC)

    # --- field seasons ---------------------------------------------------
    seasons = [
        season("mt-f11-2026-maize", "MT-F-11", "maize", t(1), t(30),
               note="reseeding and two fertilizer batches"),
        season("mt-f12-2026-soy", "MT-F-12", "soybean", t(1), t(30),
               note="harvest evidenced before the provable seeding"),
        season("mt-f13-2026-cotton", "MT-F-13", "cotton", t(1), t(30),
               note="offline backfill of an early seeding"),
        season("mt-f21-2026-maize", "MT-F-21", "maize", t(1), t(30),
               note="field with a corrected boundary"),
        season("mt-f22-2026-maize", "MT-F-22", "maize", t(1), t(30),
               note="field with overlapping boundary revisions"),
        season("mt-f23-2026-mix", "MT-F-23", "maize", t(1), t(30),
               note="field with two crops declared in the same window"),
        season("mt-f24-2026-sorghum", "MT-F-24", "sorghum", t(1), t(30),
               note="variety whose dedicated rule version was retired"),
    ]
    write("br-mt-seasons.json", {"seasons": seasons})

    # --- standardized facts ---------------------------------------------
    # All event_at values stay before the scenario "today" (mid-November) so
    # they are historical evidence; only received_at may be later, which is
    # exactly the offline-backfill case.
    facts = []
    # MT-F-11: seed, reseed, two fertilizer batches, harvest — all explainable.
    facts += [
        fact("fact-f11-seed-1", "BR-SEED-1102", "seq-f11-s1", "seeding", "MT-F-11", t(2, 7)),
        fact("fact-f11-seed-2", "BR-SEED-1102", "seq-f11-s2", "seeding", "MT-F-11", t(4, 7),
             received=t(4, 9)),
        fact("fact-f11-fert-1", "BR-FERT-3102", "seq-f11-fe1", "fertilizing", "MT-F-11", t(7, 9)),
        fact("fact-f11-fert-2", "BR-FERT-3102", "seq-f11-fe2", "fertilizing", "MT-F-11", t(12, 9)),
        fact("fact-f11-harv-1", "BR-HARV-2102", "seq-f11-h1", "harvesting", "MT-F-11", t(17, 10)),
    ]
    # MT-F-12: harvest happens before the only provable seeding.
    facts += [
        fact("fact-f12-harv-1", "BR-HARV-2102", "seq-f12-h1", "harvesting", "MT-F-12", t(3, 8)),
        fact("fact-f12-seed-1", "BR-SEED-1102", "seq-f12-s1", "seeding", "MT-F-12", t(8, 7)),
    ]
    # MT-F-13: the seeding occurred on Nov 2 but the device was offline and
    # only backfilled on Nov 12; fertilizer and harvest arrive normally.
    facts += [
        fact("fact-f13-seed-1", "BR-SEED-1102", "seq-f13-s1", "seeding", "MT-F-13",
             t(2, 7), received=t(12, 14)),
        fact("fact-f13-fert-1", "BR-FERT-3102", "seq-f13-fe1", "fertilizing", "MT-F-13",
             t(6, 9), received=t(6, 10)),
        fact("fact-f13-harv-1", "BR-HARV-2102", "seq-f13-h1", "harvesting", "MT-F-13", t(16, 10)),
    ]
    # MT-F-21/22/23/24 each get at least one attributable fact so the season
    # is reviewed rather than empty.
    facts += [
        fact("fact-f21-seed-1", "BR-SEED-1102", "seq-f21-s1", "seeding", "MT-F-21", t(4, 7)),
        fact("fact-f22-seed-1", "BR-SEED-1102", "seq-f22-s1", "seeding", "MT-F-22", t(4, 7)),
        fact("fact-f23-seed-1", "BR-SEED-1102", "seq-f23-s1", "seeding", "MT-F-23", t(4, 7)),
        fact("fact-f24-seed-1", "BR-SEED-1102", "seq-f24-s1", "seeding", "MT-F-24", t(4, 7)),
    ]
    write("br-mt-facts.json", {"facts": facts})

    # --- boundary revisions ---------------------------------------------
    boundaries = [
        # MT-F-21: revision 1 explicitly superseded by revision 2 — a clean
        # correction, not an overlap.
        boundary("br-f21-bnd-v1", "MT-F-21", t(1, 0), t(9, 0),
                 label="f21-original", superseded="br-f21-bnd-v2",
                 note="original boundary before the survey correction"),
        boundary("br-f21-bnd-v2", "MT-F-21", t(9, 0), None,
                 label="f21-corrected", note="survey correction effective Nov 9"),
        # MT-F-22: two live revisions overlap on Nov 4 when the seeding happened.
        boundary("br-f22-bnd-a", "MT-F-22", t(1, 0), t(12, 0),
                 label="f22-claim-a", note="operator A claim"),
        boundary("br-f22-bnd-b", "MT-F-22", t(3, 0), None,
                 label="f22-claim-b", note="operator B overlapping claim"),
    ]
    write("br-mt-boundaries.json", {"boundaries": boundaries})

    # --- crop calendars --------------------------------------------------
    calendars = [
        calendar("cal-f11-maize", "MT-F-11", "mt-f11-2026-maize", "maize", t(1), None),
        calendar("cal-f12-soy", "MT-F-12", "mt-f12-2026-soy", "soybean", t(1), None),
        calendar("cal-f13-cotton", "MT-F-13", "mt-f13-2026-cotton", "cotton", t(1), None),
        calendar("cal-f21-maize", "MT-F-21", "mt-f21-2026-maize", "maize", t(1), None),
        calendar("cal-f22-maize", "MT-F-22", "mt-f22-2026-maize", "maize", t(1), None),
        # MT-F-23: maize and soybean both declared live over the seeding window.
        calendar("cal-f23-maize", "MT-F-23", "mt-f23-2026-mix", "maize", t(1), None,
                 note="first crop declaration"),
        calendar("cal-f23-soy", "MT-F-23", "mt-f23-2026-mix", "soybean", t(2), None,
                 note="competing crop declaration"),
        calendar("cal-f24-sorghum", "MT-F-24", "mt-f24-2026-sorghum", "sorghum", t(1), None),
    ]
    write("br-mt-calendars.json", {"calendars": calendars})

    # --- retired sorghum-specific rule version --------------------------
    # The generic v1 sets are seeded automatically. This crop-specific FSM is
    # retired, so sorghum has no applicable rule version and the season stays
    # pending_review instead of silently falling back to the generic rules.
    retired_fsm = {
        "rule_set_id": "rules-fsm-sorghum-v1",
        "version": "rules-sorghum-v1",
        "engine": "fsm",
        "crop_code": "sorghum",
        "status": "retired",
        "rule_doc": {
            "engine": "fsm",
            "crop_code": "sorghum",
            "initial_state": "unstarted",
            "stages": ["seeded", "harvested"],
            "stage_of": {"seeding": "seeded", "harvesting": "harvested"},
            "optional_operations": [],
            "transitions": [
                {"from": "unstarted", "on": "seeding", "to": "seeded"},
                {"from": "seeded", "on": "harvesting", "to": "harvested"},
            ],
        },
    }
    write("br-mt-rules.json", {"rules": [retired_fsm]})


if __name__ == "__main__":
    main()
