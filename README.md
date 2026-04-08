# Iridium ACARS Position Inference (PoC)

This is a minimal proof-of-concept pipeline that infers aircraft position from Iridium SBD (ACARS) message fragments.

No Doppler.  
No multilateration.  
No satellite ephemeris.  

Position emerges from constraints.

---

## What This Does

- Parses Iridium `.ul` capture data
- Extracts aviation-relevant content (waypoints, routes, etc.)
- Infers a plausible aircraft position
- Outputs **Basestation (SBS)** format messages
- Streams directly into tools like `tar1090`

---

## Quick Start (10 seconds)

```bash
python3 sbd_mo_pipeline_v2_with_basestation.py synthetic_test_data.ul --basestation-out
```

You should see multiple lines of output like:

```
MSG,3,1,1,IRDM001,,,,,,,39.5,-82.0,,,,
```

---

## Stream to tar1090

If you have `tar1090` / `readsb` running:

```bash
python3 sbd_mo_pipeline_v2_with_basestation.py synthetic_test_data.ul --basestation-out | nc 127.0.0.1 30003
```

---

## Expected Result

- Aircraft appear on the map
- Callsign: `IRDM001` or others that match your synthetic_test_data.ul content
- Location: U.S.

This is synthetic data used to validate the pipeline.

---

## Files

- `sbd_mo_pipeline_v2_with_basestation.py` — main pipeline
- `synthetic_test_data.ul` — test input dataset
- 'faa_navaid_database.json' - waypoint/NAVAID database

---

## Requirements

- Python 3.x
- `nc` (netcat) for streaming (optional)
- `tar1090` / `readsb` for visualization (optional)

---

## Why This Matters

This demonstrates a different approach to geolocation:

> Observations are treated as **constraints**, not answers.

Given enough constraints, position and trajectory **emerge**—even when direct measurements are absent.

---

## Status

- Synthetic dataset: ✅ working  
- Real Iridium capture: in progress  
- Live streaming: next step  

---

## Contributing / Interested?

If you find this interesting or want to experiment, feel free to open an issue or reach out.

More to come.
