"""Build every Metabase dashboard from ``metabase_queries/`` through the REST API.

    python scripts/setup_metabase.py

One collection and one dashboard per ``metabase_queries/<NN_name>/`` folder,
one native-SQL question per ``.sql`` file in it. A file's title comes from
its file name; its chart type, size and visualization settings come from its
header comment:

    -- display: line
    -- size: 24x8
    -- settings: {"graph.dimensions": ["month"], "graph.metrics": ["claims"]}
    -- Free-text description (becomes the card's description).

Cards are laid out in file order, left to right on Metabase's 24-column
grid, wrapping to a new row when the next card doesn't fit — so number cards
go first in a folder, then the main chart, then the supporting ones.

Safe to re-run: collections, dashboards and cards are matched by name and
updated in place, and a card whose ``.sql`` file was deleted is archived.

First-time setup (the admin account) and the ClickHouse connection are done
here too. The admin credentials are read from ``MB_ADMIN_EMAIL`` /
``MB_ADMIN_PASSWORD``, from the environment or from the repo's ``.env``
file (which is git-ignored) — never from this script.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = REPO_ROOT / "metabase_queries"
GRID_WIDTH = 24

# One color per series, used for that series on every dashboard it appears
# on. Keys are result column names (or, for a breakout, the breakout value).
SERIES_COLORS = {
    "claims": "#509EE3",
    "number_of_claims": "#509EE3",
    "accidents": "#EF8C8C",
    "number_of_accidents": "#EF8C8C",
    "policies_issued": "#88BF4D",
    "policies_expired": "#A989C5",
    "net_change": "#F2A86F",
    "estimated_active_policies": "#88BF4D",
    "total_premium": "#88BF4D",
    "total_claim_amount": "#509EE3",
    "injury_claim_amount": "#ED6E6E",
    "property_claim_amount": "#F9CF48",
    "vehicle_claim_amount": "#509EE3",
    "loss_ratio_pct": "#ED6E6E",
    "3m_rolling_avg_loss_ratio_pct": "#7172AD",
    "rolling_4w_avg": "#2D3A55",
    "rolling_30d_avg_claim_amount": "#2D3A55",
    "BROOKLYN": "#509EE3",
    "QUEENS": "#88BF4D",
    "MANHATTAN": "#A989C5",
    "BRONX": "#EF8C8C",
    "STATEN ISLAND": "#F9CF48",
}

SMALL_WORDS = {"and": "&", "vs": "vs", "by": "by", "per": "per", "of": "of", "with": "with", "a": "a"}
SPECIAL_WORDS = {"p90": "P90", "30d": "30-Day", "kpi": "KPI"}


def load_dotenv(path: Path) -> None:
    """Minimal ``KEY=value`` reader, so the script has no dependency beyond requests."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def title_from_name(name: str) -> str:
    """``03_loss_ratio_by_vehicle_body`` -> ``Loss Ratio by Vehicle Body``."""
    words = re.sub(r"^\d+_", "", name).split("_")
    titled = []
    for i, word in enumerate(words):
        if word in SPECIAL_WORDS:
            titled.append(SPECIAL_WORDS[word])
        elif i > 0 and word in SMALL_WORDS:
            titled.append(SMALL_WORDS[word])
        else:
            titled.append(word[:1].upper() + word[1:])
    return " ".join(titled)


@dataclass
class CardSpec:
    name: str
    query: str
    display: str
    size_x: int
    size_y: int
    settings: dict = field(default_factory=dict)
    description: str = ""


def parse_sql_file(path: Path) -> CardSpec:
    text = path.read_text(encoding="utf-8")
    header = {}
    description = []
    for line in text.splitlines():
        if not line.startswith("--"):
            break
        body = line[2:].strip()
        match = re.match(r"^(display|size|settings):\s*(.*)$", body)
        if match:
            header[match.group(1)] = match.group(2)
        elif body:
            description.append(body)
    if "display" not in header:
        raise ValueError(f"{path}: missing '-- display:' header line")
    size_x, size_y = (int(n) for n in header.get("size", "12x6").split("x"))
    settings = json.loads(header["settings"]) if "settings" in header else {}
    return CardSpec(
        name=title_from_name(path.stem),
        query=text,
        display=header["display"],
        size_x=size_x,
        size_y=size_y,
        settings=settings,
        description=" ".join(description),
    )


def with_series_colors(settings: dict, columns: list[str]) -> dict:
    """Add ``series_settings`` colors for every result column/breakout value in ``SERIES_COLORS``
    the file didn't color itself."""
    series = dict(settings.get("series_settings", {}))
    for column in columns:
        if column in SERIES_COLORS:
            series.setdefault(column, {})
            series[column].setdefault("color", SERIES_COLORS[column])
    if not series:
        return settings
    return {**settings, "series_settings": series}


def layout(cards: list[CardSpec]) -> list[tuple[CardSpec, int, int]]:
    """(card, row, col) for each card: fill rows left to right, wrap when a card doesn't fit."""
    placed = []
    row = col = row_height = 0
    for card in cards:
        if col + card.size_x > GRID_WIDTH:
            row += row_height
            col = row_height = 0
        placed.append((card, row, col))
        col += card.size_x
        row_height = max(row_height, card.size_y)
    return placed


class Metabase:
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()

    def request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, f"{self.url}/api{path}", timeout=120, **kwargs)
        if not response.ok:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text[:500]}")
        return response.json() if response.content else None

    def wait_until_healthy(self, timeout_s: int = 300) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                if self.session.get(f"{self.url}/api/health", timeout=5).json().get("status") == "ok":
                    return
            except (requests.RequestException, ValueError):
                pass
            time.sleep(3)
        raise RuntimeError(f"Metabase at {self.url} did not become healthy within {timeout_s}s")

    def login(self, email: str, password: str) -> None:
        props = self.request("GET", "/session/properties")
        if not props.get("has-user-setup"):
            self.request(
                "POST",
                "/setup",
                json={
                    "token": props["setup-token"],
                    "user": {
                        "email": email,
                        "password": password,
                        "first_name": "Admin",
                        "last_name": "User",
                        "site_name": "Insurance Analytics",
                    },
                    "prefs": {"site_name": "Insurance Analytics", "allow_tracking": False},
                },
            )
            print("Created Metabase admin account")
        session = self.request("POST", "/session", json={"username": email, "password": password})
        self.session.headers["X-Metabase-Session"] = session["id"]

    def ensure_clickhouse(self, name: str, details: dict) -> int:
        existing = [d for d in self.request("GET", "/database")["data"] if d["name"] == name]
        payload = {"name": name, "engine": "clickhouse", "details": details}
        if existing:
            db_id = existing[0]["id"]
            self.request("PUT", f"/database/{db_id}", json=payload)
        else:
            db_id = self.request("POST", "/database", json=payload)["id"]
            print(f"Added ClickHouse database '{name}'")
        self.request("POST", f"/database/{db_id}/sync_schema")
        return db_id

    def ensure_collection(self, name: str) -> int:
        for collection in self.request("GET", "/collection"):
            if (
                collection.get("name") == name
                and not collection.get("archived")
                and collection.get("location") == "/"
            ):
                return collection["id"]
        return self.request("POST", "/collection", json={"name": name, "parent_id": None})["id"]

    def collection_items(self, collection_id: int, model: str) -> list[dict]:
        items = self.request("GET", f"/collection/{collection_id}/items", params={"models": model})
        return items["data"] if isinstance(items, dict) else items

    def result_columns(self, db_id: int, query: str) -> list[str]:
        result = self.request(
            "POST", "/dataset", json={"database": db_id, "type": "native", "native": {"query": query}}
        )
        if result.get("error"):
            raise RuntimeError(f"query failed: {result['error']}")
        return [col["name"] for col in result["data"]["cols"]]

    def upsert_card(self, collection_id: int, db_id: int, spec: CardSpec, existing: dict) -> int:
        columns = self.result_columns(db_id, spec.query)
        payload = {
            "name": spec.name,
            "description": spec.description or None,
            "display": spec.display,
            "collection_id": collection_id,
            "visualization_settings": with_series_colors(spec.settings, columns),
            "dataset_query": {
                "type": "native",
                "database": db_id,
                "native": {"query": spec.query, "template-tags": {}},
            },
        }
        if spec.name in existing:
            card_id = existing[spec.name]
            self.request("PUT", f"/card/{card_id}", json=payload)
            return card_id
        return self.request("POST", "/card", json=payload)["id"]

    def upsert_dashboard(self, collection_id: int, name: str, placed: list[tuple[int, CardSpec, int, int]]):
        existing = {d["name"]: d["id"] for d in self.collection_items(collection_id, "dashboard")}
        if name in existing:
            dashboard_id = existing[name]
        else:
            dashboard_id = self.request(
                "POST", "/dashboard", json={"name": name, "collection_id": collection_id}
            )["id"]
        dashcards = [
            {
                "id": -(i + 1),
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": spec.size_x,
                "size_y": spec.size_y,
                "parameter_mappings": [],
                "visualization_settings": {},
            }
            for i, (card_id, spec, row, col) in enumerate(placed)
        ]
        self.request("PUT", f"/dashboard/{dashboard_id}", json={"dashcards": dashcards, "width": "full"})
        return dashboard_id


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    email = os.environ.get("MB_ADMIN_EMAIL")
    password = os.environ.get("MB_ADMIN_PASSWORD")
    if not email or not password:
        print("Set MB_ADMIN_EMAIL and MB_ADMIN_PASSWORD (environment or .env).", file=sys.stderr)
        return 1

    mb = Metabase(os.environ.get("METABASE_URL", "http://localhost:3000"))
    mb.wait_until_healthy()
    mb.login(email, password)

    db_id = mb.ensure_clickhouse(
        "ClickHouse (insurance)",
        {
            # As seen from the Metabase container, on the compose network.
            "host": os.environ.get("MB_CLICKHOUSE_HOST", "clickhouse"),
            "port": int(os.environ.get("MB_CLICKHOUSE_PORT", "8123")),
            "dbname": os.environ.get("CLICKHOUSE_DATABASE", "insurance"),
            "user": os.environ.get("CLICKHOUSE_USER", "default"),
            "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
            "ssl": False,
        },
    )

    for folder in sorted(p for p in QUERIES_DIR.iterdir() if p.is_dir()):
        dashboard_name = title_from_name(folder.name)
        collection_id = mb.ensure_collection(dashboard_name)
        specs = [parse_sql_file(path) for path in sorted(folder.glob("*.sql"))]
        existing = {c["name"]: c["id"] for c in mb.collection_items(collection_id, "card")}

        placed = []
        for spec, row, col in layout(specs):
            placed.append((mb.upsert_card(collection_id, db_id, spec, existing), spec, row, col))

        wanted = {spec.name for spec in specs}
        for name, card_id in existing.items():
            if name not in wanted:
                mb.request("PUT", f"/card/{card_id}", json={"archived": True})
                print(f"  archived '{name}' (no .sql file any more)")

        dashboard_id = mb.upsert_dashboard(collection_id, dashboard_name, placed)
        print(f"{dashboard_name}: {len(placed)} cards -> {mb.url}/dashboard/{dashboard_id}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
