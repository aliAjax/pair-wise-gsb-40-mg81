"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}
LIVE_PHASES = ("reserved", "active")
FINISHED_PHASES = ("completed", "abandoned", "invalidated")

CONFLICT_MESSAGES = {
    "version_stale": "依据版本已变化",
    "asset_busy": "资源已被先到的预占占用",
    "area_taken": "搜索区域已有在执行的预占",
    "asset_unavailable": "资源当前不可用",
    "incident_not_active": "事件当前不可分配",
    "capability_missing": "资源不具备该搜索区域能力",
    "sea_state_exceeded": "海况超出资源能力",
    "range_exceeded": "搜索区域超出资源航程",
    "offline_pending_confirmation": "离线预占意图需协调员确认",
}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.details = details or {}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def validate_position(lat: Any, lon: Any) -> tuple[float, float]:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError) as exc:
        raise DomainError("经纬度必须是数值") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise DomainError("经纬度超出有效范围")
    return lat, lon


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


class MaritimeSARService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _column_exists(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        return any(row["name"] == column for row in conn.execute(f"PRAGMA table_info({table})").fetchall())

    def _add_column(self, conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        if not self._column_exists(conn, table, column):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    vessel_name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    uncertainty_km REAL NOT NULL,
                    drift_direction REAL NOT NULL DEFAULT 0,
                    drift_speed_kn REAL NOT NULL DEFAULT 0,
                    sea_state INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'reported',
                    lead_org TEXT NOT NULL,
                    duplicate_of INTEGER REFERENCES incidents(id),
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    capabilities TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    speed_kn REAL NOT NULL,
                    range_km REAL NOT NULL,
                    max_sea_state INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_areas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    code TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    center_lat REAL NOT NULL,
                    center_lon REAL NOT NULL,
                    radius_km REAL NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 3,
                    status TEXT NOT NULL DEFAULT 'planned',
                    assigned_asset_id INTEGER REFERENCES assets(id),
                    note TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS clues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER REFERENCES search_areas(id),
                    client_event_id TEXT NOT NULL UNIQUE,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    confidence REAL NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unverified',
                    distance_from_incident_km REAL NOT NULL,
                    reporter TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    merged_at TEXT,
                    summary TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    basis TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER NOT NULL REFERENCES search_areas(id),
                    asset_id INTEGER NOT NULL REFERENCES assets(id),
                    phase TEXT NOT NULL,
                    asset_version INTEGER NOT NULL,
                    incident_version INTEGER NOT NULL,
                    area_version INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    distance_km REAL NOT NULL,
                    basis TEXT NOT NULL,
                    conflicts TEXT NOT NULL DEFAULT '[]',
                    source TEXT NOT NULL DEFAULT 'online',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    reserved_at TEXT,
                    departed_at TEXT,
                    finished_at TEXT,
                    resolved_at TEXT,
                    backfilled INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS offline_event_records (
                    client_event_id TEXT PRIMARY KEY,
                    batch_id INTEGER NOT NULL REFERENCES offline_batches(id),
                    record_type TEXT NOT NULL,
                    record_id INTEGER,
                    status TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                CREATE INDEX IF NOT EXISTS idx_assignments_incident ON assignments(incident_id, phase);
                CREATE INDEX IF NOT EXISTS idx_assignments_asset ON assignments(asset_id, phase);
                """
            )
            self._add_column(conn, "offline_batches", "payload", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "offline_batches", "attempts", "INTEGER NOT NULL DEFAULT 0")
            self._add_column(conn, "offline_batches", "last_error", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "timeline", "basis", "TEXT NOT NULL DEFAULT ''")
            self._backfill_chain(conn)
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_assignments_asset_live "
                "ON assignments(asset_id) WHERE phase IN ('reserved','active')"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_assignments_area_live "
                "ON assignments(area_id) WHERE phase IN ('reserved','active')"
            )

    def _backfill_chain(self, conn: sqlite3.Connection) -> None:
        """旧数据缺协调链版本依据：按当前区域占用补一条带 backfilled 标记的依据。"""
        legacy_areas = conn.execute(
            "SELECT * FROM search_areas WHERE assigned_asset_id IS NOT NULL ORDER BY id"
        ).fetchall()
        for area in legacy_areas:
            live = conn.execute(
                "SELECT 1 FROM assignments WHERE area_id=? AND phase IN ('reserved','active')",
                (area["id"],),
            ).fetchone()
            if live:
                continue
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (area["assigned_asset_id"],)).fetchone()
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not asset or not incident:
                continue
            now = utcnow()
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            phase = "active" if area["status"] == "active" else "reserved"
            basis = self._basis_dict(incident, asset, area, distance, backfilled=True)
            conn.execute(
                """INSERT INTO assignments(incident_id,area_id,asset_id,phase,asset_version,incident_version,
                   area_version,priority,distance_km,basis,source,created_by,created_at,reserved_at,departed_at,backfilled)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                (incident["id"], area["id"], asset["id"], phase, asset["version"], incident["version"],
                 area["version"], area["priority"], round(distance, 2), json_dump(basis), "backfill",
                 "system", now, now, now if phase == "active" else None),
            )
        for row in conn.execute("SELECT * FROM timeline WHERE basis='' OR basis IS NULL").fetchall():
            details = json_loads(row["details"], {})
            basis: dict[str, Any] | None = None
            if isinstance(details, dict) and details.get("area_id"):
                assignment = conn.execute(
                    "SELECT * FROM assignments WHERE area_id=? ORDER BY id DESC LIMIT 1",
                    (details["area_id"],),
                ).fetchone()
                if assignment:
                    basis = {"backfilled": True, "assignment_id": assignment["id"],
                             "incident_version": assignment["incident_version"],
                             "asset_version": assignment["asset_version"],
                             "area_version": assignment["area_version"]}
            if basis:
                conn.execute("UPDATE timeline SET basis=? WHERE id=?", (json_dump(basis), row["id"]))

    # ---- 依据快照 ----

    def _basis_dict(self, incident: sqlite3.Row, asset: sqlite3.Row, area: sqlite3.Row,
                    distance_km: float, backfilled: bool = False) -> dict[str, Any]:
        return {
            "reserved_at": utcnow(),
            "backfilled": backfilled,
            "incident": {"id": incident["id"], "version": incident["version"], "sea_state": incident["sea_state"]},
            "asset": {
                "id": asset["id"], "version": asset["version"], "name": asset["name"], "kind": asset["kind"],
                "capabilities": json_loads(asset["capabilities"], []),
                "latitude": asset["latitude"], "longitude": asset["longitude"],
                "speed_kn": asset["speed_kn"], "range_km": asset["range_km"],
                "max_sea_state": asset["max_sea_state"], "status": asset["status"],
            },
            "area": {
                "id": area["id"], "version": area["version"], "code": area["code"], "kind": area["kind"],
                "priority": area["priority"], "center_lat": area["center_lat"],
                "center_lon": area["center_lon"], "radius_km": area["radius_km"],
            },
            "checks": {
                "distance_km": round(distance_km, 2),
                "capability": area["kind"],
                "sea_state_ok": incident["sea_state"] <= asset["max_sea_state"],
                "range_ok": distance_km <= asset["range_km"],
            },
        }

    def _version_basis(self, incident: sqlite3.Row | None = None, asset: sqlite3.Row | None = None,
                       area: sqlite3.Row | None = None, assignment_id: int | None = None,
                       backfilled: bool = False) -> dict[str, Any]:
        basis: dict[str, Any] = {}
        if incident is not None:
            basis["incident_version"] = incident["version"]
        if asset is not None:
            basis["asset_version"] = asset["version"]
        if area is not None:
            basis["area_version"] = area["version"]
        if assignment_id is not None:
            basis["assignment_id"] = assignment_id
        if backfilled:
            basis["backfilled"] = True
        return basis

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str,
               details: dict[str, Any], basis: dict[str, Any] | None = None) -> int:
        cur = conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,basis,created_at) VALUES(?,?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), json_dump(basis or {}), utcnow()),
        )
        return int(cur.lastrowid)

    # ---- 事件 ----

    def create_incident(self, actor: str, role: str, code: str, vessel_name: str,
                        latitude: float, longitude: float, uncertainty_km: float,
                        sea_state: int, lead_org: str, drift_direction: float = 0,
                        drift_speed_kn: float = 0, description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "创建遇险事件")
        code, vessel_name, lead_org = code.strip(), vessel_name.strip(), lead_org.strip()
        if not code or not vessel_name or not lead_org:
            raise DomainError("事件编号、船名和负责机构不能为空")
        lat, lon = validate_position(latitude, longitude)
        try:
            uncertainty_km = float(uncertainty_km)
            sea_state = int(sea_state)
            drift_direction = float(drift_direction)
            drift_speed_kn = float(drift_speed_kn)
        except (TypeError, ValueError) as exc:
            raise DomainError("不确定半径、海况和漂移参数必须是数值") from exc
        if uncertainty_km <= 0 or uncertainty_km > 1000:
            raise DomainError("不确定半径应在 0 到 1000 公里之间")
        if not 0 <= sea_state <= 9 or drift_speed_kn < 0:
            raise DomainError("海况或漂移速度无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT * FROM incidents WHERE vessel_name=? AND status IN ('reported','coordinating','recovering') ORDER BY id DESC",
                (vessel_name,),
            ).fetchall()
            duplicate_of = None
            for row in duplicate:
                if haversine_km(lat, lon, row["latitude"], row["longitude"]) <= max(20.0, uncertainty_km + row["uncertainty_km"]):
                    duplicate_of = row["id"]
                    break
            status = "duplicate" if duplicate_of else "reported"
            try:
                cur = conn.execute(
                    """INSERT INTO incidents(code,vessel_name,description,latitude,longitude,uncertainty_km,
                       drift_direction,drift_speed_kn,sea_state,status,lead_org,duplicate_of,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, vessel_name, description.strip(), lat, lon, uncertainty_km, drift_direction, drift_speed_kn,
                     sea_state, status, lead_org, duplicate_of, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("事件编号已存在", 409) from exc
            incident_id = int(cur.lastrowid)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            self._audit(conn, incident_id, actor, "incident.reported",
                        {"duplicate_of": duplicate_of}, self._version_basis(incident))
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected",
                            {"duplicate_incident": code})
            return dict(incident)

    def update_sea_state(self, actor: str, role: str, incident_id: int, sea_state: int,
                         expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "更新海况")
        try:
            sea_state, expected_version = int(sea_state), int(expected_version)
        except (TypeError, ValueError) as exc:
            raise DomainError("海况和版本必须是整数") from exc
        if not 0 <= sea_state <= 9:
            raise DomainError("海况无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != expected_version:
                raise DomainError("事件已变化，请刷新后重试", 409)
            old_state = incident["sea_state"]
            conn.execute(
                "UPDATE incidents SET sea_state=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (sea_state, utcnow(), incident_id, expected_version),
            )
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            self._audit(conn, incident_id, actor, "incident.sea_state_changed",
                        {"from": old_state, "to": sea_state}, self._version_basis(incident))
            recompute = {}
            if old_state != sea_state:
                recompute = self._recompute_conn(conn, actor, [incident_id], "sea_state")
            return dict(incident) | {"recompute": recompute}

    # ---- 资源 ----

    def list_assets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def add_asset(self, actor: str, role: str, name: str, kind: str,
                  capabilities: list[str], latitude: float, longitude: float,
                  speed_kn: float, range_km: float, max_sea_state: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "登记搜救资源")
        lat, lon = validate_position(latitude, longitude)
        name, kind = name.strip(), kind.strip()
        caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
        if not name or not kind or not caps:
            raise DomainError("资源名称、类型和能力不能为空")
        try:
            speed_kn, range_km, max_sea_state = float(speed_kn), float(range_km), int(max_sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("速度和航程参数必须是数值") from exc
        if speed_kn <= 0 or range_km <= 0 or not 0 <= max_sea_state <= 9:
            raise DomainError("速度、航程或适用海况无效")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO assets(name,kind,capabilities,latitude,longitude,speed_kn,range_km,max_sea_state,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (name, kind, json_dump(caps), lat, lon, speed_kn, range_km, max_sea_state, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("资源名称已存在", 409) from exc
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone()
            self._audit(conn, None, actor, "asset.registered",
                        {"asset_id": cur.lastrowid, "name": name}, self._version_basis(asset=asset))
            return dict(asset)

    def update_asset(self, actor: str, role: str, asset_id: int, expected_version: int,
                     latitude: float | None = None, longitude: float | None = None,
                     capabilities: list[str] | None = None, range_km: float | None = None,
                     max_sea_state: int | None = None, speed_kn: float | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "更新资源状态")
        try:
            expected_version = int(expected_version)
        except (TypeError, ValueError) as exc:
            raise DomainError("资源版本必须是整数") from exc
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != expected_version:
                raise DomainError("资源状态已变化，先到者已生效，请刷新后重试", 409)
            new_lat, new_lon = asset["latitude"], asset["longitude"]
            if latitude is not None or longitude is not None:
                new_lat, new_lon = validate_position(
                    asset["latitude"] if latitude is None else latitude,
                    asset["longitude"] if longitude is None else longitude,
                )
            new_caps = json_loads(asset["capabilities"], [])
            if capabilities is not None:
                new_caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
                if not new_caps:
                    raise DomainError("资源能力不能为空")
            try:
                new_range = float(range_km) if range_km is not None else asset["range_km"]
                new_max = int(max_sea_state) if max_sea_state is not None else asset["max_sea_state"]
                new_speed = float(speed_kn) if speed_kn is not None else asset["speed_kn"]
            except (TypeError, ValueError) as exc:
                raise DomainError("速度和航程参数必须是数值") from exc
            if new_range <= 0 or new_speed <= 0 or not 0 <= new_max <= 9:
                raise DomainError("速度、航程或适用海况无效")
            before = {"latitude": asset["latitude"], "longitude": asset["longitude"],
                      "capabilities": json_loads(asset["capabilities"], []), "range_km": asset["range_km"],
                      "max_sea_state": asset["max_sea_state"], "speed_kn": asset["speed_kn"]}
            conn.execute(
                """UPDATE assets SET latitude=?,longitude=?,capabilities=?,range_km=?,max_sea_state=?,speed_kn=?,
                   version=version+1,updated_at=? WHERE id=? AND version=?""",
                (new_lat, new_lon, json_dump(new_caps), new_range, new_max, new_speed, utcnow(),
                 asset_id, expected_version),
            )
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            affected = [row["incident_id"] for row in conn.execute(
                "SELECT DISTINCT incident_id FROM assignments WHERE asset_id=? AND phase IN ('reserved','active')",
                (asset_id,),
            ).fetchall()]
            self._audit(conn, None, actor, "asset.updated",
                        {"asset_id": asset_id, "before": before,
                         "after": {"latitude": new_lat, "longitude": new_lon, "capabilities": new_caps,
                                   "range_km": new_range, "max_sea_state": new_max, "speed_kn": new_speed}},
                        self._version_basis(asset=asset))
            recompute = self._recompute_conn(conn, actor, affected, "asset") if affected else {}
            return dict(asset) | {"recompute": recompute}

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        if not reason.strip():
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            # 先标记撤回，重算时不会再次占用该资源
            conn.execute("UPDATE assets SET status='withdrawn',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            affected = [row["incident_id"] for row in conn.execute(
                "SELECT DISTINCT incident_id FROM assignments WHERE asset_id=? AND phase IN ('reserved','active')",
                (asset_id,),
            ).fetchall()]
            self._audit(conn, None, actor, "asset.withdrawn",
                        {"asset_id": asset_id, "reason": reason.strip(), "affected_incidents": affected},
                        self._version_basis(asset=conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()))
            recompute = self._recompute_conn(conn, actor, affected, "asset_withdrawn") if affected else {}
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()) | {"recompute": recompute}

    # ---- 搜索区域 ----

    def create_search_area(self, actor: str, role: str, incident_id: int, code: str,
                           kind: str, center_lat: float, center_lon: float,
                           radius_km: float, priority: int = 3, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建搜索区域")
        lat, lon = validate_position(center_lat, center_lon)
        kind, code = kind.strip(), code.strip()
        if not kind or not code:
            raise DomainError("区域类型和编号不能为空")
        try:
            radius_km, priority = float(radius_km), int(priority)
        except (TypeError, ValueError) as exc:
            raise DomainError("半径和优先级必须是数值") from exc
        if radius_km <= 0 or not 1 <= priority <= 5:
            raise DomainError("搜索半径或优先级无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("当前事件不能创建搜索区域", 409)
            try:
                cur = conn.execute(
                    """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,note,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (incident_id, code, kind, lat, lon, radius_km, priority, note.strip(), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("搜索区域编号已存在", 409) from exc
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone()
            self._audit(conn, incident_id, actor, "area.created",
                        {"area_id": cur.lastrowid, "code": code, "priority": priority},
                        self._version_basis(incident, area=area))
            return dict(area)

    def update_area(self, actor: str, role: str, area_id: int, expected_version: int,
                    kind: str | None = None, center_lat: float | None = None,
                    center_lon: float | None = None, radius_km: float | None = None,
                    priority: int | None = None, note: str | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "更新搜索区域")
        try:
            expected_version = int(expected_version)
        except (TypeError, ValueError) as exc:
            raise DomainError("区域版本必须是整数") from exc
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["version"] != expected_version:
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            new_kind = (kind.strip() if isinstance(kind, str) and kind.strip() else area["kind"])
            if center_lat is not None or center_lon is not None:
                new_lat, new_lon = validate_position(
                    area["center_lat"] if center_lat is None else center_lat,
                    area["center_lon"] if center_lon is None else center_lon,
                )
            else:
                new_lat, new_lon = area["center_lat"], area["center_lon"]
            try:
                new_radius = float(radius_km) if radius_km is not None else area["radius_km"]
                new_priority = int(priority) if priority is not None else area["priority"]
            except (TypeError, ValueError) as exc:
                raise DomainError("半径和优先级必须是数值") from exc
            if new_radius <= 0 or not 1 <= new_priority <= 5:
                raise DomainError("搜索半径或优先级无效")
            new_note = area["note"] if note is None else note.strip()
            conn.execute(
                """UPDATE search_areas SET kind=?,center_lat=?,center_lon=?,radius_km=?,priority=?,note=?,
                   version=version+1,updated_at=? WHERE id=? AND version=?""",
                (new_kind, new_lat, new_lon, new_radius, new_priority, new_note, utcnow(),
                 area_id, expected_version),
            )
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            self._audit(conn, area["incident_id"], actor, "area.updated",
                        {"area_id": area_id}, self._version_basis(area=area))
            recompute = self._recompute_conn(conn, actor, [area["incident_id"]], "area")
            return dict(area) | {"recompute": recompute}

    # ---- 协调链：预占 / 候选 / 出发 / 重算 ----

    def _eligibility(self, incident: sqlite3.Row, asset: sqlite3.Row,
                     area: sqlite3.Row) -> tuple[float, list[dict[str, str]]]:
        conflicts: list[dict[str, str]] = []
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if area["kind"] not in json_loads(asset["capabilities"], []):
            conflicts.append({"code": "capability_missing", "message": CONFLICT_MESSAGES["capability_missing"],
                              "required": area["kind"], "asset_id": asset["id"]})
        if incident["sea_state"] > asset["max_sea_state"]:
            conflicts.append({"code": "sea_state_exceeded", "message": CONFLICT_MESSAGES["sea_state_exceeded"],
                              "sea_state": incident["sea_state"], "max_sea_state": asset["max_sea_state"],
                              "asset_id": asset["id"]})
        if distance > asset["range_km"]:
            conflicts.append({"code": "range_exceeded", "message": CONFLICT_MESSAGES["range_exceeded"],
                              "distance_km": round(distance, 2), "range_km": asset["range_km"],
                              "asset_id": asset["id"]})
        return distance, conflicts

    def _live_assignment(self, conn: sqlite3.Connection, *, area_id: int | None = None,
                         asset_id: int | None = None) -> sqlite3.Row | None:
        if area_id is not None:
            return conn.execute(
                "SELECT * FROM assignments WHERE area_id=? AND phase IN ('reserved','active') ORDER BY id DESC LIMIT 1",
                (area_id,),
            ).fetchone()
        if asset_id is not None:
            return conn.execute(
                "SELECT * FROM assignments WHERE asset_id=? AND phase IN ('reserved','active') ORDER BY id DESC LIMIT 1",
                (asset_id,),
            ).fetchone()
        return None

    def _queue_candidate(self, conn: sqlite3.Connection, actor: str, incident: sqlite3.Row,
                         asset: sqlite3.Row, area: sqlite3.Row, distance_km: float,
                         conflicts: list[dict[str, Any]], source: str, now: str) -> int:
        basis = self._basis_dict(incident, asset, area, distance_km)
        cur = conn.execute(
            """INSERT INTO assignments(incident_id,area_id,asset_id,phase,asset_version,incident_version,
               area_version,priority,distance_km,basis,conflicts,source,created_by,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident["id"], area["id"], asset["id"], "candidate", asset["version"], incident["version"],
             area["version"], area["priority"], round(distance_km, 2), json_dump(basis),
             json_dump(conflicts), source, actor, now),
        )
        return int(cur.lastrowid)

    def _conflict_error(self, conflicts: list[dict[str, Any]]) -> str:
        return "；".join(item.get("message", item.get("code", "冲突")) for item in conflicts) or "预占冲突"

    def _attempt_reservation(self, conn: sqlite3.Connection, actor: str, area_id: int, asset_id: int,
                             expected_asset_version: int | None = None,
                             expected_area_version: int | None = None,
                             source: str = "online",
                             commit_on_conflict: bool = True,
                             queue_on_conflict: bool = True) -> sqlite3.Row:
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        now = utcnow()
        distance, conflicts = self._eligibility(incident, asset, area)
        if incident["status"] not in ACTIVE_INCIDENT:
            conflicts.insert(0, {"code": "incident_not_active", "message": CONFLICT_MESSAGES["incident_not_active"]})
        if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
            conflicts.insert(0, {"code": "version_stale", "message": CONFLICT_MESSAGES["version_stale"],
                                 "expected_asset_version": int(expected_asset_version),
                                 "actual_asset_version": asset["version"], "asset_id": asset_id})
        if expected_area_version is not None and area["version"] != int(expected_area_version):
            conflicts.insert(0, {"code": "version_stale", "message": CONFLICT_MESSAGES["version_stale"],
                                 "expected_area_version": int(expected_area_version),
                                 "actual_area_version": area["version"], "area_id": area_id})
        holder_asset = self._live_assignment(conn, asset_id=asset_id)
        if holder_asset is not None:
            conflicts.append({"code": "asset_busy", "message": CONFLICT_MESSAGES["asset_busy"],
                              "asset_id": asset_id, "holder_assignment_id": holder_asset["id"],
                              "holder_area_id": holder_asset["area_id"]})
        holder_area = self._live_assignment(conn, area_id=area_id)
        if holder_area is not None:
            conflicts.append({"code": "area_taken", "message": CONFLICT_MESSAGES["area_taken"],
                              "area_id": area_id, "holder_assignment_id": holder_area["id"],
                              "holder_asset_id": holder_area["asset_id"]})
        if asset["status"] != "available":
            conflicts.append({"code": "asset_unavailable", "message": CONFLICT_MESSAGES["asset_unavailable"],
                              "asset_id": asset_id, "status": asset["status"]})
        if conflicts:
            details: dict[str, Any] = {"conflicts": conflicts}
            if queue_on_conflict:
                candidate_id = self._queue_candidate(conn, actor, incident, asset, area, distance, conflicts, source, now)
                self._audit(conn, incident["id"], actor, "assignment.candidate_queued",
                            {"candidate_id": candidate_id, "area_id": area_id, "asset_id": asset_id,
                             "source": source, "conflicts": conflicts},
                            self._version_basis(incident, asset, area))
                details["candidate_id"] = candidate_id
            if commit_on_conflict:
                conn.commit()
            raise DomainError(self._conflict_error(conflicts), 409, details)

        changed = conn.execute(
            "UPDATE assets SET status='assigned',version=version+1,updated_at=? "
            "WHERE id=? AND status='available'",
            (now, asset_id),
        )
        if changed.rowcount != 1:
            conflicts = [{"code": "asset_busy", "message": CONFLICT_MESSAGES["asset_busy"], "asset_id": asset_id}]
            raise DomainError(self._conflict_error(conflicts), 409, {"conflicts": conflicts})
        basis = self._basis_dict(incident, asset, area, distance)
        try:
            cur = conn.execute(
                """INSERT INTO assignments(incident_id,area_id,asset_id,phase,asset_version,incident_version,
                   area_version,priority,distance_km,basis,source,created_by,created_at,reserved_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident["id"], area_id, asset_id, "reserved", asset["version"], incident["version"],
                 area["version"], area["priority"], round(distance, 2), json_dump(basis),
                 source, actor, now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise DomainError("资源或区域已被先到者占用", 409,
                              {"conflicts": [{"code": "asset_busy", "message": CONFLICT_MESSAGES["asset_busy"]}]}) from exc
        assignment_id = int(cur.lastrowid)
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
            (asset_id, now, area_id),
        )
        self._audit(conn, incident["id"], actor, "area.assigned",
                    {"area_id": area_id, "asset_id": asset_id, "assignment_id": assignment_id,
                     "distance_km": round(distance, 2), "source": source},
                    {"assignment_id": assignment_id,
                     "incident_version": incident["version"],
                     "asset_version": asset["version"],
                     "area_version": area["version"]})
        return conn.execute(
            "SELECT * FROM assignments WHERE area_id=? AND phase IN ('reserved','active') ORDER BY id DESC LIMIT 1",
            (area_id,),
        ).fetchone()

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None,
                    expected_area_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            assignment = self._attempt_reservation(
                conn, actor, area_id, asset_id, expected_asset_version, expected_area_version
            )
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            return dict(area) | {"assignment_id": assignment["id"]}

    def accept_candidate(self, actor: str, role: str, candidate_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "采纳候选预占")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            candidate = conn.execute("SELECT * FROM assignments WHERE id=? AND phase='candidate'", (candidate_id,)).fetchone()
            if not candidate:
                raise DomainError("候选不存在或已处理", 404)
            if candidate["resolved_at"]:
                raise DomainError("候选已经处理过", 409)
            try:
                assignment = self._attempt_reservation(
                    conn, actor, candidate["area_id"], candidate["asset_id"],
                    source="candidate", commit_on_conflict=False, queue_on_conflict=False
                )
            except DomainError as exc:
                raise DomainError("候选当前仍无法生效：" + str(exc), exc.status, exc.details) from exc
            conn.execute("UPDATE assignments SET resolved_at=? WHERE id=?", (utcnow(), candidate_id))
            self._audit(conn, candidate["incident_id"], actor, "assignment.candidate_accepted",
                        {"candidate_id": candidate_id, "assignment_id": assignment["id"]},
                        {"assignment_id": assignment["id"]})
            return self._assignment_dict(conn, assignment)

    def dispatch_assignment(self, actor: str, role: str, area_id: int,
                            expected_area_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "下达出发指令")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            assignment = self._live_assignment(conn, area_id=area_id)
            if not assignment:
                raise DomainError("区域没有生效中的预占", 409)
            if assignment["phase"] == "active":
                raise DomainError("任务已经出发，原依据保留", 409)
            if expected_area_version is not None and area["version"] != int(expected_area_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            now = utcnow()
            conn.execute(
                "UPDATE assignments SET phase='active',departed_at=? WHERE id=?", (now, assignment["id"])
            )
            conn.execute(
                "UPDATE search_areas SET status='active',version=version+1,updated_at=? WHERE id=?", (now, area_id)
            )
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            self._audit(conn, area["incident_id"], actor, "area.dispatched",
                        {"area_id": area_id, "asset_id": assignment["asset_id"],
                         "assignment_id": assignment["id"]},
                        {"assignment_id": assignment["id"],
                         "incident_version": assignment["incident_version"],
                         "asset_version": assignment["asset_version"],
                         "area_version": assignment["area_version"]})
            return self._assignment_dict(
                conn, conn.execute("SELECT * FROM assignments WHERE id=?", (assignment["id"],)).fetchone()
            )

    def _plan_incident_conn(self, conn: sqlite3.Connection, actor: str,
                            incident: sqlite3.Row) -> list[dict[str, Any]]:
        """按区域优先级从未预占区域开始，挑可达且能力/海况合格的空闲资源预占。"""
        outcomes: list[dict[str, Any]] = []
        areas = conn.execute(
            "SELECT * FROM search_areas WHERE incident_id=? AND status='planned' ORDER BY priority,id",
            (incident["id"],),
        ).fetchall()
        available = conn.execute("SELECT * FROM assets WHERE status='available' ORDER BY id").fetchall()
        for area in areas:
            fits: list[tuple[float, sqlite3.Row]] = []
            near_misses: list[dict[str, Any]] = []
            for asset in available:
                distance, conflicts = self._eligibility(incident, asset, area)
                if not conflicts:
                    fits.append((distance, asset))
                else:
                    near_misses.append({"asset_id": asset["id"], "conflicts": conflicts})
            if not fits:
                outcomes.append({"area_id": area["id"], "priority": area["priority"], "status": "unmet",
                                 "conflicts": near_misses[:5]})
                self._audit(conn, incident["id"], actor, "assignment.plan_unmet",
                            {"area_id": area["id"], "priority": area["priority"], "candidates": near_misses[:5]},
                            self._version_basis(incident, area=area))
                continue
            fits.sort(key=lambda item: (item[0], item[1]["id"]))
            distance, asset = fits[0]
            try:
                assignment = self._attempt_reservation(
                    conn, actor, area["id"], asset["id"], source="plan", commit_on_conflict=False
                )
            except DomainError as exc:
                outcomes.append({"area_id": area["id"], "priority": area["priority"], "status": "conflict",
                                 "conflicts": exc.details.get("conflicts", [])})
                continue
            outcomes.append({"area_id": area["id"], "priority": area["priority"], "status": "reserved",
                             "asset_id": asset["id"], "assignment_id": assignment["id"],
                             "distance_km": round(distance, 2)})
        return outcomes

    def plan_incident(self, actor: str, role: str, incident_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "协调链预占")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("当前事件不能进行预占", 409)
            outcomes = self._plan_incident_conn(conn, actor, incident)
            self._audit(conn, incident_id, actor, "incident.planned",
                        {"outcomes": outcomes}, self._version_basis(incident))
            return {"incident_id": incident_id, "outcomes": outcomes}

    def _recompute_conn(self, conn: sqlite3.Connection, actor: str, incident_ids: list[int],
                        reason: str) -> dict[str, Any]:
        """依据变化：未出发预占失效并按优先级重算；已出发任务保留原依据。"""
        result: dict[str, Any] = {"reason": reason, "invalidated": [], "rebooked": [],
                                  "unmet": [], "kept_departed": []}
        now = utcnow()
        for incident_id in sorted(set(incident_ids)):
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                continue
            for assignment in conn.execute(
                "SELECT * FROM assignments WHERE incident_id=? AND phase='reserved' ORDER BY priority,id",
                (incident_id,),
            ).fetchall():
                conn.execute(
                    "UPDATE assets SET status='available',version=version+1,updated_at=? "
                    "WHERE id=? AND status='assigned'",
                    (now, assignment["asset_id"]),
                )
                conn.execute(
                    "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?",
                    (now, assignment["area_id"]),
                )
                conn.execute("UPDATE assignments SET phase='invalidated',finished_at=? WHERE id=?", (now, assignment["id"]))
                self._audit(conn, incident_id, actor, "area.invalidated",
                            {"area_id": assignment["area_id"], "asset_id": assignment["asset_id"],
                             "assignment_id": assignment["id"], "reason": reason,
                             "basis_versions": {"incident_version": assignment["incident_version"],
                                                "asset_version": assignment["asset_version"],
                                                "area_version": assignment["area_version"]}},
                            {"assignment_id": assignment["id"],
                             "incident_version": assignment["incident_version"],
                             "asset_version": assignment["asset_version"],
                             "area_version": assignment["area_version"]})
                result["invalidated"].append({"assignment_id": assignment["id"], "area_id": assignment["area_id"],
                                              "asset_id": assignment["asset_id"]})
            for active in conn.execute(
                "SELECT * FROM assignments WHERE incident_id=? AND phase='active'", (incident_id,)
            ).fetchall():
                result["kept_departed"].append(
                    {"assignment_id": active["id"], "area_id": active["area_id"], "asset_id": active["asset_id"],
                     "basis_versions": {"incident_version": active["incident_version"],
                                        "asset_version": active["asset_version"],
                                        "area_version": active["area_version"]}}
                )
            if incident["status"] in ACTIVE_INCIDENT:
                for outcome in self._plan_incident_conn(conn, actor, incident):
                    if outcome["status"] == "reserved":
                        result["rebooked"].append(outcome)
                    else:
                        result["unmet"].append(outcome)
            self._audit(conn, incident_id, actor, "chain.recomputed",
                        {"reason": reason, **{k: v for k, v in result.items() if k != "reason"}},
                        self._version_basis(incident))
        return result

    # ---- 线索 ----

    def record_clue(self, actor: str, role: str, incident_id: int, client_event_id: str,
                    latitude: float, longitude: float, confidence: float, source: str,
                    area_id: int | None = None, details: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator", "field"}, "记录搜索线索")
        lat, lon = validate_position(latitude, longitude)
        event_id, source = client_event_id.strip(), source.strip()
        if not event_id or not source:
            raise DomainError("事件幂等编号和线索来源不能为空")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError) as exc:
            raise DomainError("线索置信度必须是数值") from exc
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            if existing:
                return dict(existing)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能新增线索", 409)
            area = None
            if area_id is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)).fetchone()
                if not area:
                    raise DomainError("搜索区域不属于该事件", 409)
            distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
            status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
            cur = conn.execute(
                """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                   distance_from_incident_km,reporter,details,recorded_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident_id, area_id, event_id, lat, lon, confidence, source, status, distance, actor, details.strip(), utcnow()),
            )
            self._audit(conn, incident_id, actor, "clue.recorded",
                        {"clue_id": cur.lastrowid, "status": status, "event_id": event_id},
                        self._version_basis(incident, area=area))
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (cur.lastrowid,)).fetchone())

    def verify_clue(self, actor: str, role: str, clue_id: int, status: str,
                    expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "analyst"}, "核验线索")
        if status not in {"verified", "rejected", "unverified"}:
            raise DomainError("线索状态无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            clue = conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone()
            if not clue:
                raise DomainError("线索不存在", 404)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    # ---- 事件流转 ----

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            self._audit(conn, incident_id, actor, "incident.transferred",
                        {"from": incident["lead_org"], "to": new_org, "note": note.strip()},
                        self._version_basis(incident))
            return dict(incident)

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            assignment = self._live_assignment(conn, area_id=area_id)
            now = utcnow()
            if assignment is not None:
                conn.execute(
                    "UPDATE assignments SET phase=?,finished_at=? WHERE id=?",
                    (outcome, now, assignment["id"]),
                )
                conn.execute(
                    "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                    (now, assignment["asset_id"]),
                )
            conn.execute(
                "UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?",
                (outcome, now, area_id),
            )
            self._audit(conn, area["incident_id"], actor, "area." + outcome,
                        {"area_id": area_id, "assignment_id": assignment["id"] if assignment else None},
                        {"assignment_id": assignment["id"]} if assignment else None)
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            self._audit(conn, incident_id, actor, "incident.closed",
                        {"outcome": outcome}, self._version_basis(incident))
            return dict(incident)

    # ---- 离线批次：先落完整载荷，按事件号幂等，失败可从完整批次恢复 ----

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        payload = json_dump({"events": events})
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
            if existing is None:
                now = utcnow()
                conn.execute(
                    "INSERT OR IGNORE INTO offline_batches(client_batch_id,actor,status,received_at,summary,payload,attempts) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (batch_id, actor, "staged", now, "[]", payload, 1),
                )
                existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
                if existing["status"] != "staged":
                    conn.commit()
                    if existing["status"] == "merged":
                        return {"batch_id": batch_id, "idempotent": True, "status": "merged",
                                "summary": json_loads(existing["summary"], {})}
                batch_pk = int(existing["id"])
                conn.commit()
            else:
                batch_pk = existing["id"]
                if existing["status"] == "merged":
                    return {"batch_id": batch_id, "idempotent": True, "status": "merged",
                            "summary": json_loads(existing["summary"], {})}
        # staged/failed：从已保存的完整批次恢复重放
        try:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                batch = conn.execute("SELECT * FROM offline_batches WHERE id=?", (batch_pk,)).fetchone()
                if batch["status"] == "merged":
                    conn.commit()
                    return {"batch_id": batch_id, "idempotent": True, "status": "merged",
                            "summary": json_loads(batch["summary"], {})}
                stored_events = json_loads(batch["payload"], {}).get("events", events)
                was_recovery = batch["status"] == "failed" or batch["attempts"] > 1
                conn.execute("UPDATE offline_batches SET attempts=attempts+1,last_error='',status='staged' WHERE id=?",
                             (batch_pk,))
                results = self._apply_offline_events(conn, actor, role, batch_pk, stored_events)
                summary = {
                    "accepted": sum(1 for item in results if item["status"] in ("merged", "reserved")),
                    "rejected": sum(1 for item in results if item["status"] == "rejected"),
                    "candidates": sum(1 for item in results if item["status"] == "candidate"),
                    "deduplicated": sum(1 for item in results if item.get("idempotent")),
                    "events": results,
                }
                now = utcnow()
                conn.execute(
                    "UPDATE offline_batches SET status='merged',merged_at=?,summary=? WHERE id=?",
                    (now, json_dump(summary), batch_pk),
                )
                self._audit(conn, None, actor, "offline.batch_merged",
                            {"batch_id": batch_id, "attempts": batch["attempts"] + 1,
                             **{k: summary[k] for k in ("accepted", "rejected", "candidates", "deduplicated")}})
                return {"batch_id": batch_id, "idempotent": False, "status": "merged",
                        "recovered": was_recovery, "summary": summary}
        except Exception as exc:
            with self.connect() as conn:
                conn.execute(
                    "UPDATE offline_batches SET status='failed',last_error=? WHERE id=?",
                    (str(exc)[:500], batch_pk),
                )
            if isinstance(exc, DomainError):
                raise
            raise DomainError("批次写入失败，完整批次已暂存，重新提交同一批次编号即可恢复", 500,
                              {"batch_id": batch_id, "recover": "重新提交相同 client_batch_id"}) from exc

    def _apply_offline_events(self, conn: sqlite3.Connection, actor: str, role: str,
                              batch_pk: int, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        now = utcnow()
        for event in events:
            event_id = str(event.get("client_event_id", "")).strip()
            if not event_id:
                results.append({"client_event_id": event_id, "status": "rejected",
                                "error": "离线事件缺少 client_event_id"})
                continue
            known = conn.execute("SELECT * FROM offline_event_records WHERE client_event_id=?", (event_id,)).fetchone()
            if known:
                results.append({"client_event_id": event_id, "status": "deduplicated",
                                "record_id": known["record_id"], "idempotent": True,
                                "batch_id": known["batch_id"]})
                continue
            try:
                event_type = event.get("type")
                if event_type == "clue":
                    result = self._apply_offline_clue(conn, actor, event_id, event)
                elif event_type == "timeline":
                    incident_id = int(event["incident_id"])
                    if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
                        raise DomainError("事件不存在", 404)
                    timeline_id = self._audit(conn, incident_id, actor,
                                              event.get("action", "offline.note"),
                                              event.get("details", {}))
                    result = {"status": "merged", "record_id": timeline_id}
                elif event_type == "assignment":
                    result = self._apply_offline_assignment(conn, actor, role, event_id, event)
                else:
                    raise DomainError("不支持的离线事件类型")
                conn.execute(
                    "INSERT INTO offline_event_records(client_event_id,batch_id,record_type,record_id,status,applied_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (event_id, batch_pk, event_type, result.get("record_id"), result["status"], now),
                )
                results.append({"client_event_id": event_id, **result})
            except (DomainError, KeyError, TypeError, ValueError) as exc:
                results.append({"client_event_id": event_id, "status": "rejected", "error": str(exc)})
        return results

    def _apply_offline_clue(self, conn: sqlite3.Connection, actor: str, event_id: str,
                            event: dict[str, Any]) -> dict[str, Any]:
        existing_clue = conn.execute("SELECT id FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
        if existing_clue:
            return {"status": "merged", "record_id": existing_clue["id"], "idempotent": True}
        incident_id = int(event["incident_id"])
        lat, lon = validate_position(event["latitude"], event["longitude"])
        confidence = float(event["confidence"])
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if not incident:
            raise DomainError("事件不存在", 404)
        if incident["status"] in CLOSED_INCIDENT:
            raise DomainError("已结束事件不能新增线索", 409)
        area_id = event.get("area_id")
        area = None
        if area_id is not None:
            area = conn.execute(
                "SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
            ).fetchone()
            if not area:
                raise DomainError("搜索区域不属于该事件", 409)
        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
        cur = conn.execute(
            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,details,recorded_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (incident_id, area_id, event_id, lat, lon, confidence,
             str(event.get("source", "offline")).strip(), status, distance, actor,
             str(event.get("details", "")).strip(), utcnow()),
        )
        self._audit(conn, incident_id, actor, "clue.recorded",
                    {"clue_id": cur.lastrowid, "status": status, "event_id": event_id, "offline": True},
                    self._version_basis(incident, area=area))
        return {"status": "merged", "record_id": cur.lastrowid}

    def _apply_offline_assignment(self, conn: sqlite3.Connection, actor: str, role: str,
                                  event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        area_id, asset_id = int(event["area_id"]), int(event["asset_id"])
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if role != "coordinator":
            distance, conflicts = self._eligibility(incident, asset, area)
            conflicts.append({"code": "offline_pending_confirmation",
                              "message": CONFLICT_MESSAGES["offline_pending_confirmation"]})
            candidate_id = self._queue_candidate(conn, actor, incident, asset, area, distance,
                                                 conflicts, "offline", utcnow())
            return {"status": "candidate", "record_id": candidate_id, "conflicts": conflicts}
        try:
            assignment = self._attempt_reservation(
                conn, actor, area_id, asset_id, source="offline", commit_on_conflict=False
            )
        except DomainError as exc:
            return {"status": "candidate",
                    "record_id": exc.details.get("candidate_id"),
                    "conflicts": exc.details.get("conflicts", [])}
        return {"status": "reserved", "record_id": assignment["id"]}

    # ---- 视图：状态 / 时间线 / 导出（同一版本依据） ----

    def _assignment_dict(self, conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["basis"] = json_loads(row["basis"], {})
        data["conflicts"] = json_loads(row["conflicts"], [])
        return data

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
            assignments = [self._assignment_dict(conn, r)
                           for r in conn.execute("SELECT * FROM assignments ORDER BY id DESC LIMIT 500").fetchall()]
            batches = [dict(r) | {"summary": json_loads(r["summary"], {})}
                       for r in conn.execute("SELECT * FROM offline_batches ORDER BY id DESC LIMIT 100").fetchall()]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "timeline": timeline, "assignments": assignments, "offline_batches": batches}

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    def export_chain(self, incident_id: int | None = None) -> dict[str, Any]:
        state = self.state()
        if incident_id is not None:
            area_ids = {a["id"] for a in state["search_areas"] if a["incident_id"] == incident_id}
            state["incidents"] = [i for i in state["incidents"] if i["id"] == incident_id]
            state["search_areas"] = [a for a in state["search_areas"] if a["incident_id"] == incident_id]
            state["clues"] = [c for c in state["clues"] if c["incident_id"] == incident_id]
            state["timeline"] = [t for t in state["timeline"] if t["incident_id"] == incident_id]
            state["assignments"] = [a for a in state["assignments"] if a["incident_id"] == incident_id]
            asset_ids = {a["asset_id"] for a in state["assignments"]} | {a["assigned_asset_id"] for a in state["search_areas"]}
            asset_ids.discard(None)
            state["assets"] = [a for a in state["assets"] if a["id"] in asset_ids]
        return {"generated_at": utcnow(), "incident_id": incident_id, **state}

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        return {"seeded": True, "incident_id": incident["id"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: MaritimeSARService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _actor(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "viewer")

    def _json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise DomainError("Content-Length 无效") from exc
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(data, dict):
            raise DomainError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/health":
                self._send(200, {"status": "ok", "service": "maritime-sar"})
                return
            if path == "/api/state":
                self._send(200, self.service.state(*self._actor()))
                return
            if path == "/api/export":
                query = parse_qs(parsed.query)
                incident_id = int(query["incident_id"][0]) if query.get("incident_id") else None
                self._send(200, self.service.export_chain(incident_id))
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc), **getattr(exc, "details", {})})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/assets/update":
                result = self.service.update_asset(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/areas/update":
                result = self.service.update_area(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/assignments/dispatch":
                result = self.service.dispatch_assignment(actor, role, **data)
            elif path == "/api/candidates/accept":
                result = self.service.accept_candidate(actor, role, **data)
            elif path == "/api/incidents/plan":
                result = self.service.plan_incident(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/incidents/sea-state":
                result = self.service.update_sea_state(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc), **exc.details})
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: MaritimeSARService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Maritime SAR service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="海上搜救协调服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8206)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = MaritimeSARService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
