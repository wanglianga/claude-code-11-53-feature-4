#!/usr/bin/env python3
"""Generate the deterministic multi-region sample payloads under samples/.

Every region uploads a batch of work segments for the 2026-11-01..2026-11-15
period. Trajectories (raw points) and device alarms are attached to every
segment so residency policy has something to actually filter.

Usage: python3 scripts/gen_samples.py
"""
import json
import os
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "samples")

UTC = timezone.utc
BASE = datetime(2026, 11, 1, 6, 0, tzinfo=UTC)


def iso(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


REGIONS = {
    "DE-BB": {
        "devices": [("DE-SEED-1001", "seeding", "seeder"), ("DE-HARV-2001", "harvesting", "harvester"),
                    ("DE-FERT-3001", "fertilizing", "spreader")],
        "fields": ["BB-F-01", "BB-F-02", "BB-F-03"],
        "base_lat": 52.41, "base_lon": 13.25,
        "area": [48.5, 62.25, 31.75], "fuel": [410.0, 520.5, 180.25],
        "duration": [5400, 7200, 3300],
    },
    "BR-MT": {
        "devices": [("BR-SEED-1102", "seeding", "seeder"), ("BR-HARV-2102", "harvesting", "harvester"),
                    ("BR-FERT-3102", "fertilizing", "spreader")],
        "fields": ["MT-F-11", "MT-F-12", "MT-F-13"],
        "base_lat": -12.64, "base_lon": -55.42,
        "area": [120.0, 145.5, 88.25], "fuel": [980.0, 1120.75, 410.0],
        "duration": [9000, 10800, 5400],
    },
    "US-IA": {
        "devices": [("US-SEED-1203", "seeding", "seeder"), ("US-HARV-2203", "harvesting", "harvester"),
                    ("US-FERT-3203", "fertilizing", "spreader")],
        "fields": ["IA-F-21", "IA-F-22", "IA-F-23"],
        "base_lat": 41.88, "base_lon": -93.10,
        "area": [96.0, 141.75, 73.5], "fuel": [760.0, 890.0, 300.5],
        "duration": [7200, 9000, 4200],
    },
    "FR-NOR": {
        "devices": [("FR-SEED-1304", "seeding", "seeder"), ("FR-HARV-2304", "harvesting", "harvester"),
                    ("FR-FERT-3304", "fertilizing", "spreader")],
        "fields": ["NOR-F-31", "NOR-F-32", "NOR-F-33"],
        "base_lat": 49.18, "base_lon": 0.37,
        "area": [54.0, 66.5, 41.25], "fuel": [380.0, 455.25, 190.0],
        "duration": [5400, 6600, 3000],
    },
}


def points(lat, lon, start, n=6):
    out = []
    for i in range(n):
        out.append({
            "lat": round(lat + i * 0.0012, 6),
            "lon": round(lon + i * 0.0009, 6),
            "at": iso(start + timedelta(minutes=7 * i)),
        })
    return out


def build_region(code, spec, batch_id, segments, start_offset_hours, backfill=False):
    ops = []
    for idx, seg in enumerate(segments):
        dev, op_type, _kind = spec["devices"][idx % len(spec["devices"])]
        start = BASE + timedelta(hours=start_offset_hours + idx * 9)
        dur = spec["duration"][idx % len(spec["duration"])]
        end = start + timedelta(seconds=dur)
        alarms = []
        if seg.get("alarm"):
            alarms.append({
                "code": seg["alarm"],
                "severity": seg.get("severity", "warning"),
                "occurred_at": iso(start + timedelta(minutes=25)),
            })
        ops.append({
            "device_serial": dev,
            "segment_id": f"{code}-{batch_id}-{seg['seq']}",
            "operation_type": op_type,
            "field_id": spec["fields"][idx % len(spec["fields"])],
            "area_ha": seg["area"],
            "fuel_l": seg["fuel"],
            "duration_s": seg.get("duration", dur),
            "completed": seg.get("completed", True),
            "started_at": iso(start),
            "ended_at": iso(end),
            "points": points(spec["base_lat"], spec["base_lon"], start),
            "alarms": alarms,
        })
    return {"batch_id": batch_id, "operations": ops}


def seg(seq, area, fuel, **kw):
    out = {"seq": seq, "area": area, "fuel": fuel}
    out.update(kw)
    return out


def write(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    print("wrote", os.path.relpath(path, ROOT))


def main():
    # Each region's initial 4 uploaded segments.
    plan = {
        "DE-BB": [seg(1, 48.5, 410.0), seg(2, 62.25, 520.5, alarm="HYDRAULIC_PRESSURE_LOW"),
                  seg(3, 31.75, 180.25), seg(4, 22.0, 130.0, completed=False)],
        "BR-MT": [seg(1, 120.0, 980.0, alarm="SEED_FLOW_BLOCKED", severity="critical"),
                  seg(2, 145.5, 1120.75), seg(3, 88.25, 410.0), seg(4, 60.5, 295.0)],
        "US-IA": [seg(1, 96.0, 760.0), seg(2, 141.75, 890.0, alarm="ENGINE_TEMP_HIGH"),
                  seg(3, 73.5, 300.5), seg(4, 52.0, 240.0, completed=False)],
        "FR-NOR": [seg(1, 54.0, 380.0), seg(2, 66.5, 455.25), seg(3, 41.25, 190.0,
                  alarm="GPS_SIGNAL_DEGRADED", severity="info"), seg(4, 38.0, 175.0)],
    }
    for code, spec in REGIONS.items():
        batch = build_region(code, spec, "initial", plan[code], start_offset_hours=list(REGIONS).index(code) * 2)
        write(os.path.join(OUT, "regions", f"{code.lower()}.json"), batch)

    # BR-MT keeps working while its inter-region link is down. These segments
    # are committed locally and copied only once the link is restored.
    offline = build_region("BR-MT", REGIONS["BR-MT"], "offline", [
        seg(5, 77.5, 350.0), seg(6, 95.25, 420.0, alarm="FUEL_LEVEL_CRITICAL", severity="critical"),
        seg(7, 110.0, 505.5),
    ], start_offset_hours=80)
    write(os.path.join(OUT, "offline", "br-mt-offline-backfill.json"), offline)

    # A retry upload that repeats three already-accepted BR-MT segments and
    # adds one new segment. The repeated operations keep the exact device
    # serials they had in the backfill batch, because dedupe keys on
    # (device_serial, segment_id) within the region. The region must report
    # the repeats as duplicates and the group area total must not change.
    repeats = build_region("BR-MT", REGIONS["BR-MT"], "offline", [
        seg(5, 77.5, 350.0), seg(6, 95.25, 420.0, alarm="FUEL_LEVEL_CRITICAL", severity="critical"),
        seg(7, 110.0, 505.5),
    ], start_offset_hours=80)
    new_seg = build_region("BR-MT", REGIONS["BR-MT"], "retry", [seg(8, 66.0, 310.0)],
                           start_offset_hours=104)
    dup_batch = {
        "batch_id": "br-mt-retry-with-overlap",
        "operations": repeats["operations"] + new_seg["operations"],
    }
    write(os.path.join(OUT, "offline", "br-mt-retry-overlap.json"), dup_batch)

    # Second batch for US-IA used after its primary/standby switch, to prove the
    # local write path keeps working on the promoted node.
    post_switch = build_region("US-IA", REGIONS["US-IA"], "post-switch", [
        seg(5, 58.25, 265.0), seg(6, 44.5, 205.75),
    ], start_offset_hours=130)
    write(os.path.join(OUT, "dr", "us-ia-post-switch.json"), post_switch)

    # Outbound approvals covering different scopes.
    wide = {
        "purpose": "Group agronomy weekly completion review",
        "recipient": "group-agronomy@example.org",
        "scope_regions": ["BR-MT", "DE-BB", "FR-NOR", "US-IA"],
        "scope_metrics": ["operation_count", "completed_count", "completion_rate", "area_ha", "fuel_l", "duration_hours"],
        "valid_until": "2026-12-31T23:59:59Z",
    }
    write(os.path.join(OUT, "approvals", "wide.json"), wide)

    narrow = {
        "purpose": "Cross-region comparison for DE-BB, BR-MT and US-IA only",
        "recipient": "regional-ops@example.org",
        "scope_regions": ["BR-MT", "DE-BB", "US-IA"],
        "scope_metrics": ["operation_count", "completion_rate", "area_ha"],
        "valid_until": "2026-12-31T23:59:59Z",
    }
    write(os.path.join(OUT, "approvals", "narrow.json"), narrow)


if __name__ == "__main__":
    main()
