"""Builds the initial-load file (bulk_telecom_events.json) that goes to GitHub -> ADF."""
import json
import random
import sys

from data import generate_telecom_event

n = int(sys.argv[1]) if len(sys.argv) > 1 else 4000
random.seed(7)
events = [generate_telecom_event(days_back=30) for _ in range(n)]
with open("bulk_telecom_events.json", "w", encoding="utf-8") as f:
    json.dump(events, f, indent=2, ensure_ascii=False)
print(f"wrote {n} events")
