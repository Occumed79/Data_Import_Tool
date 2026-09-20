from __future__ import annotations

import json

from src.sources import refresh_all_active_sources


if __name__ == "__main__":
    results = refresh_all_active_sources(due_only=True)
    print(json.dumps(results, indent=2, default=str))
