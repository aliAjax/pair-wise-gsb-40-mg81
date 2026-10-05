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
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}
OPEN_ASSIGNMENT = {"planned", "departed"}


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
                    version INTEGER NOT NULL DEFAULT 1,
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
                    payload TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_event_id TEXT UNIQUE,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER NOT NULL REFERENCES search_areas(id),
                    asset_id INTEGER NOT NULL REFERENCES assets(id),
                    status TEXT NOT NULL DEFAULT 'planned',
                    basis TEXT NOT NULL,
                    backfilled INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    departed_at TEXT,
                    closed_at TEXT,
                    close_reason TEXT
                );
                CREATE TABLE IF NOT EXISTS assignment_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER NOT NULL REFERENCES search_areas(id),
                    asset_id INTEGER NOT NULL REFERENCES assets(id),
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    conflicts TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                CREATE INDEX IF NOT EXISTS idx_assignments_area ON assignments(area_id, status);
                CREATE INDEX IF NOT EXISTS idx_assignments_asset ON assignments(asset_id, status);
                """
            )
            self._migrate(conn)

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(%s)" % table)}
        if column not in columns:
            conn.execute(ddl)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """旧库回填：补版本列、补批次原始负载列、为历史占用补建派遣任务依据。"""
        self._ensure_column(conn, "clues", "version",
                            "ALTER TABLE clues ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
        self._ensure_column(conn, "offline_batches", "payload",
                            "ALTER TABLE offline_batches ADD COLUMN payload TEXT NOT NULL DEFAULT ''")
        legacy = conn.execute(
            """SELECT a.* FROM search_areas a
               WHERE a.assigned_asset_id IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM assignments s
                                 WHERE s.area_id=a.id AND s.status IN ('planned','departed'))"""
        ).fetchall()
        for area in legacy:
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (area["assigned_asset_id"],)).fetchone()
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not asset or not incident:
                continue
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            basis = self._basis_snapshot(incident, area, asset, distance, "system")
            basis["backfilled"] = True
            status = "departed" if area["status"] == "active" else "planned"
            now = utcnow()
            cur = conn.execute(
                """INSERT INTO assignments(incident_id,area_id,asset_id,status,basis,backfilled,created_by,created_at,departed_at)
                   VALUES(?,?,?,?,?,1,'system',?,?)""",
                (area["incident_id"], area["id"], asset["id"], status, json_dump(basis), now,
                 now if status == "departed" else None),
            )
            self._audit(conn, area["incident_id"], "system", "assignment.backfilled",
                        {"assignment_id": cur.lastrowid, "area_id": area["id"], "asset_id": asset["id"]})

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), utcnow()),
        )

    def _basis_snapshot(self, incident: sqlite3.Row, area: sqlite3.Row, asset: sqlite3.Row,
                        distance: float, actor: str) -> dict[str, Any]:
        """下达时刻的版本依据：能力、航程、海况与区域版本快照，已出发任务永久保留。"""
        return {
            "incident_id": incident["id"],
            "sea_state": incident["sea_state"],
            "area_id": area["id"],
            "area_version": area["version"],
            "area_priority": area["priority"],
            "area_kind": area["kind"],
            "asset_id": asset["id"],
            "asset_version": asset["version"],
            "asset_latitude": asset["latitude"],
            "asset_longitude": asset["longitude"],
            "range_km": asset["range_km"],
            "max_sea_state": asset["max_sea_state"],
            "capabilities": json.loads(asset["capabilities"]),
            "distance_km": round(distance, 2),
            "decided_by": actor,
            "decided_at": utcnow(),
        }

    def _record_candidate(self, conn: sqlite3.Connection, actor: str, area: sqlite3.Row,
                          asset_id: int, conflicts: list[str]) -> int:
        cur = conn.execute(
            """INSERT INTO assignment_candidates(incident_id,area_id,asset_id,actor,status,conflicts,created_at)
               VALUES(?,?,?,?,'pending',?,?)""",
            (area["incident_id"], area["id"], asset_id, actor, json_dump(conflicts), utcnow()),
        )
        return int(cur.lastrowid)

    def _assign_locked(self, conn: sqlite3.Connection, actor: str, area_id: int, asset_id: int,
                       expected_asset_version: int | None = None, client_event_id: str | None = None,
                       cause: str = "manual", track_candidate: bool = True) -> tuple[int, dict[str, Any]]:
        """按当时能力、航程与海况预占资源。冲突时保留候选并列出冲突（先到者生效）。"""
        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        if not area or not asset:
            raise DomainError("搜索区域或资源不存在", 404)
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        conflicts: list[str] = []
        if area["assigned_asset_id"] is not None:
            conflicts.append("搜索区域 %s 已分配资源" % area["code"])
        if not incident or incident["status"] not in ACTIVE_INCIDENT:
            conflicts.append("事件当前不可分配")
        if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
            conflicts.append("资源版本已变化：期望 %s，当前 %s" % (expected_asset_version, asset["version"]))
        if asset["status"] != "available":
            holder = conn.execute(
                "SELECT id FROM assignments WHERE asset_id=? AND status IN ('planned','departed') ORDER BY id DESC LIMIT 1",
                (asset_id,),
            ).fetchone()
            message = "资源当前不可用（%s）" % asset["status"]
            if holder:
                message += "，被任务 #%s 占用" % holder["id"]
            conflicts.append(message)
        if incident and incident["sea_state"] > asset["max_sea_state"]:
            conflicts.append("海况 %d 超出资源适用海况 %d" % (incident["sea_state"], asset["max_sea_state"]))
        if area["kind"] not in json.loads(asset["capabilities"]):
            conflicts.append("资源不具备 %s 搜索能力" % area["kind"])
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if distance > asset["range_km"]:
            conflicts.append("搜索区域超出资源航程：%.1f > %.1f 公里" % (distance, asset["range_km"]))
        if conflicts:
            candidate_id = self._record_candidate(conn, actor, area, asset_id, conflicts) if track_candidate else None
            if track_candidate:
                self._audit(conn, area["incident_id"], actor, "assignment.conflict",
                            {"area_id": area_id, "asset_id": asset_id, "candidate_id": candidate_id, "conflicts": conflicts})
            raise DomainError("分配冲突：" + "；".join(conflicts), 409,
                              {"conflicts": conflicts, "candidate_id": candidate_id})
        now = utcnow()
        changed = conn.execute(
            "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
            (now, asset_id, asset["version"]),
        )
        if changed.rowcount != 1:
            conflicts = ["资源已被其他值班员占用"]
            candidate_id = self._record_candidate(conn, actor, area, asset_id, conflicts) if track_candidate else None
            raise DomainError("分配冲突：" + conflicts[0], 409,
                              {"conflicts": conflicts, "candidate_id": candidate_id})
        conn.execute(
            "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
            (asset_id, now, area_id),
        )
        asset_now = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
        area_now = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
        basis = self._basis_snapshot(incident, area_now, asset_now, distance, actor)
        cur = conn.execute(
            """INSERT INTO assignments(client_event_id,incident_id,area_id,asset_id,status,basis,created_by,created_at)
               VALUES(?,?,?,?,'planned',?,?,?)""",
            (client_event_id, area["incident_id"], area_id, asset_id, json_dump(basis), actor, now),
        )
        assignment_id = int(cur.lastrowid)
        self._audit(conn, area["incident_id"], actor, "area.assigned",
                    {"area_id": area_id, "asset_id": asset_id, "assignment_id": assignment_id,
                     "distance_km": round(distance, 2), "cause": cause, "basis": basis})
        return assignment_id, basis

    def _revalidate_planned(self, conn: sqlite3.Connection, actor: str, cause: str) -> None:
        """海况、资源状态或区域版本变化后：未出发任务失效并按区域优先级重算，已出发任务保留原依据。"""
        stale: list[tuple[sqlite3.Row, list[str]]] = []
        rows = conn.execute("SELECT * FROM assignments WHERE status='planned'").fetchall()
        for assignment in rows:
            basis = json.loads(assignment["basis"])
            reasons: list[str] = []
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (assignment["incident_id"],)).fetchone()
            if incident and incident["sea_state"] != basis.get("sea_state"):
                reasons.append("海况 %s→%s" % (basis.get("sea_state"), incident["sea_state"]))
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (assignment["asset_id"],)).fetchone()
            if asset and asset["version"] != basis.get("asset_version"):
                reasons.append("资源状态变化（版本 %s→%s）" % (basis.get("asset_version"), asset["version"]))
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (assignment["area_id"],)).fetchone()
            if area and area["version"] != basis.get("area_version"):
                reasons.append("区域版本变化（%s→%s）" % (basis.get("area_version"), area["version"]))
            if reasons:
                stale.append((assignment, reasons))
        now = utcnow()
        freed_area_ids: list[int] = []
        for assignment, reasons in stale:
            conn.execute(
                "UPDATE assignments SET status='invalidated',closed_at=?,close_reason=? WHERE id=?",
                (now, "；".join(reasons), assignment["id"]),
            )
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=? AND assigned_asset_id=?",
                (now, assignment["area_id"], assignment["asset_id"]),
            )
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (assignment["asset_id"],)).fetchone()
            if asset and asset["status"] == "assigned":
                other = conn.execute(
                    "SELECT 1 FROM assignments WHERE asset_id=? AND status IN ('planned','departed') AND id<>?",
                    (assignment["asset_id"], assignment["id"]),
                ).fetchone()
                if not other:
                    conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                                 (now, assignment["asset_id"]))
            self._audit(conn, assignment["incident_id"], actor, "assignment.invalidated",
                        {"assignment_id": assignment["id"], "area_id": assignment["area_id"],
                         "asset_id": assignment["asset_id"], "cause": cause, "reasons": reasons})
            freed_area_ids.append(assignment["area_id"])
        if not freed_area_ids:
            return
        placeholders = ",".join("?" for _ in freed_area_ids)
        areas = conn.execute(
            "SELECT * FROM search_areas WHERE id IN (%s) ORDER BY priority,id" % placeholders,
            tuple(freed_area_ids),
        ).fetchall()
        for area in areas:
            self._replan_area(conn, actor, area, cause)

    def _replan_area(self, conn: sqlite3.Connection, actor: str, area: sqlite3.Row, cause: str) -> int | None:
        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
        if not incident or incident["status"] not in ACTIVE_INCIDENT:
            return None
        feasible: list[tuple[float, int]] = []
        rejected: list[dict[str, str]] = []
        for asset in conn.execute("SELECT * FROM assets WHERE status='available' ORDER BY id").fetchall():
            if area["kind"] not in json.loads(asset["capabilities"]):
                rejected.append({"asset": asset["name"], "reason": "不具备 %s 能力" % area["kind"]})
                continue
            if incident["sea_state"] > asset["max_sea_state"]:
                rejected.append({"asset": asset["name"], "reason": "海况超出能力"})
                continue
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                rejected.append({"asset": asset["name"], "reason": "超出航程"})
                continue
            feasible.append((distance, asset["id"]))
        if not feasible:
            self._audit(conn, area["incident_id"], actor, "area.replan_pending",
                        {"area_id": area["id"], "cause": cause, "rejected": rejected})
            return None
        feasible.sort()
        _, best_asset_id = feasible[0]
        assignment_id, _ = self._assign_locked(conn, actor, area["id"], best_asset_id,
                                               cause="replan:" + cause, track_candidate=False)
        return assignment_id

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
            self._audit(conn, incident_id, actor, "incident.reported", {"duplicate_of": duplicate_of})
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected", {"duplicate_incident": code})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

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
            self._audit(conn, None, actor, "asset.registered", {"asset_id": cur.lastrowid, "name": name})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone())

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
            self._audit(conn, incident_id, actor, "area.created", {"area_id": cur.lastrowid, "code": code})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone())

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None,
                    client_event_id: str | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if client_event_id:
                existing = conn.execute("SELECT * FROM assignments WHERE client_event_id=?", (client_event_id,)).fetchone()
                if existing:
                    area = conn.execute("SELECT * FROM search_areas WHERE id=?", (existing["area_id"],)).fetchone()
                    result = dict(area)
                    result.update({"assignment_id": existing["id"], "basis": json.loads(existing["basis"]), "idempotent": True})
                    return result
            try:
                assignment_id, basis = self._assign_locked(conn, actor, area_id, asset_id,
                                                           expected_asset_version, client_event_id)
            except DomainError:
                conn.commit()  # 冲突候选与审计需落库，不能随异常回滚
                raise
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            result = dict(area)
            result.update({"assignment_id": assignment_id, "basis": basis})
            return result

    def depart_assignment(self, actor: str, role: str, assignment_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "任务出发")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            assignment = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
            if not assignment:
                raise DomainError("派遣任务不存在", 404)
            if assignment["status"] != "planned":
                raise DomainError("任务已出发或已结束", 409)
            now = utcnow()
            changed = conn.execute(
                "UPDATE assignments SET status='departed',departed_at=? WHERE id=? AND status='planned'",
                (now, assignment_id),
            )
            if changed.rowcount != 1:
                raise DomainError("任务状态已变化，请刷新后重试", 409)
            conn.execute("UPDATE search_areas SET status='active',version=version+1,updated_at=? WHERE id=?",
                         (now, assignment["area_id"]))
            basis = json.loads(assignment["basis"])
            self._audit(conn, assignment["incident_id"], actor, "assignment.departed",
                        {"assignment_id": assignment_id, "area_id": assignment["area_id"],
                         "asset_id": assignment["asset_id"], "basis": basis})
            result = dict(conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone())
            result["basis"] = basis
            return result

    def update_sea_state(self, actor: str, role: str, incident_id: int, sea_state: int,
                         expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "更新海况")
        try:
            sea_state = int(sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("海况必须是数值") from exc
        if not 0 <= sea_state <= 9:
            raise DomainError("海况无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能更新海况", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute("UPDATE incidents SET sea_state=?,version=version+1,updated_at=? WHERE id=?",
                         (sea_state, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.sea_state_updated",
                        {"from": incident["sea_state"], "to": sea_state})
            self._revalidate_planned(conn, actor, "sea_state")
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def update_asset(self, actor: str, role: str, asset_id: int, expected_version: int,
                     latitude: float | None = None, longitude: float | None = None,
                     speed_kn: float | None = None, range_km: float | None = None,
                     max_sea_state: int | None = None, capabilities: list[str] | None = None,
                     status: str | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "更新资源状态")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            updates: dict[str, Any] = {}
            if latitude is not None or longitude is not None:
                if latitude is None or longitude is None:
                    raise DomainError("经纬度必须同时提供")
                updates["latitude"], updates["longitude"] = validate_position(latitude, longitude)
            if speed_kn is not None:
                speed_kn = float(speed_kn)
                if speed_kn <= 0:
                    raise DomainError("速度无效")
                updates["speed_kn"] = speed_kn
            if range_km is not None:
                range_km = float(range_km)
                if range_km <= 0:
                    raise DomainError("航程无效")
                updates["range_km"] = range_km
            if max_sea_state is not None:
                max_sea_state = int(max_sea_state)
                if not 0 <= max_sea_state <= 9:
                    raise DomainError("适用海况无效")
                updates["max_sea_state"] = max_sea_state
            if capabilities is not None:
                caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
                if not caps:
                    raise DomainError("能力不能为空")
                updates["capabilities"] = json_dump(caps)
            if status is not None:
                if status not in {"available", "maintenance"}:
                    raise DomainError("资源状态只能设为 available 或 maintenance")
                if status == "available" and asset["status"] == "assigned":
                    raise DomainError("资源仍有任务，请先撤回", 409)
                updates["status"] = status
            if not updates:
                raise DomainError("没有需要更新的字段")
            changed = {key: (json.loads(value) if key == "capabilities" else value) for key, value in updates.items()}
            updates["version"] = asset["version"] + 1
            updates["updated_at"] = utcnow()
            clause = ",".join("%s=?" % key for key in updates)
            conn.execute("UPDATE assets SET %s WHERE id=?" % clause, (*updates.values(), asset_id))
            self._audit(conn, None, actor, "asset.updated", {"asset_id": asset_id, "changed": changed})
            self._revalidate_planned(conn, actor, "asset_updated")
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    def update_area(self, actor: str, role: str, area_id: int, expected_version: int,
                    priority: int | None = None, center_lat: float | None = None,
                    center_lon: float | None = None, radius_km: float | None = None,
                    kind: str | None = None, note: str | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "更新搜索区域")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            updates: dict[str, Any] = {}
            if priority is not None:
                priority = int(priority)
                if not 1 <= priority <= 5:
                    raise DomainError("优先级无效")
                updates["priority"] = priority
            if center_lat is not None or center_lon is not None:
                if center_lat is None or center_lon is None:
                    raise DomainError("区域中心经纬度必须同时提供")
                updates["center_lat"], updates["center_lon"] = validate_position(center_lat, center_lon)
            if radius_km is not None:
                radius_km = float(radius_km)
                if radius_km <= 0:
                    raise DomainError("搜索半径无效")
                updates["radius_km"] = radius_km
            if kind is not None:
                kind = kind.strip()
                if not kind:
                    raise DomainError("区域类型不能为空")
                updates["kind"] = kind
            if note is not None:
                updates["note"] = note.strip()
            if not updates:
                raise DomainError("没有需要更新的字段")
            updates["version"] = area["version"] + 1
            updates["updated_at"] = utcnow()
            clause = ",".join("%s=?" % key for key in updates)
            conn.execute("UPDATE search_areas SET %s WHERE id=?" % clause, (*updates.values(), area_id))
            self._audit(conn, area["incident_id"], actor, "area.updated",
                        {"area_id": area_id, "changed": {k: v for k, v in updates.items() if k not in {"version", "updated_at"}}})
            self._revalidate_planned(conn, actor, "area_updated")
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def retry_candidate(self, actor: str, role: str, candidate_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "重试候选分配")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            candidate = conn.execute("SELECT * FROM assignment_candidates WHERE id=?", (candidate_id,)).fetchone()
            if not candidate:
                raise DomainError("候选不存在", 404)
            if candidate["status"] != "pending":
                raise DomainError("候选已处理", 409)
            try:
                assignment_id, basis = self._assign_locked(conn, actor, candidate["area_id"], candidate["asset_id"],
                                                           cause="candidate", track_candidate=False)
            except DomainError as exc:
                if exc.details.get("conflicts"):
                    conn.execute("UPDATE assignment_candidates SET conflicts=? WHERE id=?",
                                 (json_dump(exc.details["conflicts"]), candidate_id))
                    conn.commit()  # 保留最新冲突清单
                raise
            conn.execute("UPDATE assignment_candidates SET status='promoted',resolved_at=? WHERE id=?",
                         (utcnow(), candidate_id))
            return {"candidate_id": candidate_id, "assignment_id": assignment_id, "basis": basis}

    def dismiss_candidate(self, actor: str, role: str, candidate_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "放弃候选分配")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            candidate = conn.execute("SELECT * FROM assignment_candidates WHERE id=?", (candidate_id,)).fetchone()
            if not candidate:
                raise DomainError("候选不存在", 404)
            if candidate["status"] != "pending":
                raise DomainError("候选已处理", 409)
            conn.execute("UPDATE assignment_candidates SET status='dismissed',resolved_at=? WHERE id=?",
                         (utcnow(), candidate_id))
            self._audit(conn, candidate["incident_id"], actor, "assignment.candidate_dismissed",
                        {"candidate_id": candidate_id, "area_id": candidate["area_id"], "asset_id": candidate["asset_id"]})
            return dict(conn.execute("SELECT * FROM assignment_candidates WHERE id=?", (candidate_id,)).fetchone())

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
            self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
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
            if expected_version is not None and clue["version"] != int(expected_version):
                raise DomainError("线索已变化，请刷新后重试", 409)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,version=version+1,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

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
            if asset["status"] != "assigned":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            areas = conn.execute("SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')", (asset_id,)).fetchall()
            for area in areas:
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?", (now, area["id"]))
                self._audit(conn, area["incident_id"], actor, "area.unassigned", {"area_id": area["id"], "reason": reason.strip()})
            open_assignments = conn.execute(
                "SELECT id,incident_id FROM assignments WHERE asset_id=? AND status IN ('planned','departed')", (asset_id,)
            ).fetchall()
            for assignment in open_assignments:
                conn.execute("UPDATE assignments SET status='released',closed_at=?,close_reason=? WHERE id=?",
                             (now, reason.strip(), assignment["id"]))
                self._audit(conn, assignment["incident_id"], actor, "assignment.released",
                            {"assignment_id": assignment["id"], "reason": reason.strip()})
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn", {"asset_id": asset_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

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
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

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
            if area["assigned_asset_id"] is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=? AND status='assigned'",
                             (utcnow(), area["assigned_asset_id"]))
            open_assignment = conn.execute(
                "SELECT id FROM assignments WHERE area_id=? AND status IN ('planned','departed') ORDER BY id DESC LIMIT 1",
                (area_id,),
            ).fetchone()
            if open_assignment:
                conn.execute("UPDATE assignments SET status=?,closed_at=?,close_reason=? WHERE id=?",
                             ("completed" if outcome == "completed" else "released", utcnow(), outcome, open_assignment["id"]))
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?", (outcome, utcnow(), area_id))
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
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
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
            if existing and existing["status"] == "merged":
                return {"batch_id": batch_id, "idempotent": True, "status": "merged",
                        "summary": json.loads(existing["summary"])}
            if existing is None:
                conn.execute(
                    "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,summary,payload) VALUES(?,?,?,?,?,?)",
                    (batch_id, actor, "received", utcnow(), "{}", json_dump(events)),
                )
                stored_events = events
            else:
                # 未完成的批次：忽略本次请求体，从已落库的完整批次恢复
                stored_events = json.loads(existing["payload"] or "[]")
        try:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                results = [self._merge_offline_event(conn, actor, event) for event in stored_events]
                summary = {"accepted": sum(1 for item in results if item["status"] == "merged"),
                           "rejected": sum(1 for item in results if item["status"] == "rejected"),
                           "events": results}
                now = utcnow()
                conn.execute("UPDATE offline_batches SET status='merged',merged_at=?,summary=? WHERE client_batch_id=?",
                             (now, json_dump(summary), batch_id))
                self._audit(conn, None, actor, "offline.batch_merged",
                            {"batch_id": batch_id, **{k: summary[k] for k in ("accepted", "rejected")}})
                return {"batch_id": batch_id, "idempotent": False, "status": "merged", "summary": summary}
        except DomainError:
            raise
        except Exception as exc:
            with self.connect() as conn:
                conn.execute("UPDATE offline_batches SET status='failed',summary=? WHERE client_batch_id=?",
                             (json_dump({"error": str(exc)}), batch_id))
            raise DomainError("批次写入失败，可使用相同批次号重试恢复", 500) from exc

    def _merge_offline_event(self, conn: sqlite3.Connection, actor: str, event: Any) -> dict[str, Any]:
        conn.execute("SAVEPOINT offline_event")
        event_id = ""
        try:
            if not isinstance(event, dict):
                raise DomainError("离线事件格式无效")
            event_id = str(event.get("client_event_id", "")).strip()
            if not event_id:
                raise DomainError("离线事件缺少 client_event_id")
            event_type = event.get("type")
            if event_type == "clue":
                result = self._merge_offline_clue(conn, actor, event, event_id)
            elif event_type == "timeline":
                incident_id = int(event["incident_id"])
                if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
                    raise DomainError("事件不存在", 404)
                self._audit(conn, incident_id, actor, event.get("action", "offline.note"), event.get("details", {}))
                result = {"client_event_id": event_id, "status": "merged", "record_id": None}
            elif event_type == "assignment":
                result = self._merge_offline_assignment(conn, actor, event, event_id)
            else:
                raise DomainError("不支持的离线事件类型")
            conn.execute("RELEASE offline_event")
            return result
        except (DomainError, KeyError, TypeError, ValueError) as exc:
            conn.execute("ROLLBACK TO offline_event")
            conn.execute("RELEASE offline_event")
            rejected: dict[str, Any] = {"client_event_id": event_id, "status": "rejected", "error": str(exc)}
            if isinstance(exc, DomainError) and exc.details.get("conflicts"):
                rejected["conflicts"] = exc.details["conflicts"]
            return rejected

    def _merge_offline_clue(self, conn: sqlite3.Connection, actor: str,
                            event: dict[str, Any], event_id: str) -> dict[str, Any]:
        existing_clue = conn.execute("SELECT id FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
        if existing_clue:
            return {"client_event_id": event_id, "status": "merged", "record_id": existing_clue["id"], "idempotent": True}
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
        if area_id is not None and not conn.execute(
            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
        ).fetchone():
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
        self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
        return {"client_event_id": event_id, "status": "merged", "record_id": cur.lastrowid}

    def _merge_offline_assignment(self, conn: sqlite3.Connection, actor: str,
                                  event: dict[str, Any], event_id: str) -> dict[str, Any]:
        existing = conn.execute("SELECT id FROM assignments WHERE client_event_id=?", (event_id,)).fetchone()
        if existing:
            return {"client_event_id": event_id, "status": "merged", "record_id": existing["id"], "idempotent": True}
        assignment_id, _ = self._assign_locked(conn, actor, int(event["area_id"]), int(event["asset_id"]),
                                               client_event_id=event_id, cause="offline", track_candidate=False)
        return {"client_event_id": event_id, "status": "merged", "record_id": assignment_id}

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            assignments = [dict(r) for r in conn.execute("SELECT * FROM assignments ORDER BY id DESC LIMIT 200").fetchall()]
            candidates = [dict(r) for r in conn.execute("SELECT * FROM assignment_candidates ORDER BY id DESC LIMIT 100").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
        for assignment in assignments:
            assignment["basis"] = json.loads(assignment["basis"])
        for candidate in candidates:
            candidate["conflicts"] = json.loads(candidate["conflicts"])
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "assignments": assignments, "candidates": candidates, "timeline": timeline}

    def export_state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        """导出与页面、时间线相同的版本依据快照。"""
        data = self.state(actor, role)
        data["generated_at"] = utcnow()
        return data

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

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
            path = urlparse(self.path).path
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
                self._send(200, self.service.export_state(*self._actor()))
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/assignments/depart":
                result = self.service.depart_assignment(actor, role, **data)
            elif path == "/api/assignments/candidates/retry":
                result = self.service.retry_candidate(actor, role, **data)
            elif path == "/api/assignments/candidates/dismiss":
                result = self.service.dismiss_candidate(actor, role, **data)
            elif path == "/api/incidents/sea_state":
                result = self.service.update_sea_state(actor, role, **data)
            elif path == "/api/assets/update":
                result = self.service.update_asset(actor, role, **data)
            elif path == "/api/areas/update":
                result = self.service.update_area(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            payload = {"error": str(exc)}
            payload.update(exc.details)
            self._send(exc.status, payload)
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
