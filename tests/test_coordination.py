import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService, utcnow  # noqa: E402

OLD_SCHEMA = """
CREATE TABLE incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    vessel_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    uncertainty_km NOT NULL,
    drift_direction REAL NOT NULL DEFAULT 0,
    drift_speed_kn REAL NOT NULL DEFAULT 0,
    sea_state INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'reported',
    lead_org TEXT NOT NULL,
    duplicate_of INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE assets (
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
CREATE TABLE search_areas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER NOT NULL,
    code TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    center_lat REAL NOT NULL,
    center_lon REAL NOT NULL,
    radius_km REAL NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    status TEXT NOT NULL DEFAULT 'planned',
    assigned_asset_id INTEGER,
    note TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE clues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER NOT NULL,
    area_id INTEGER,
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
CREATE TABLE offline_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_batch_id TEXT NOT NULL UNIQUE,
    actor TEXT NOT NULL,
    status TEXT NOT NULL,
    received_at TEXT NOT NULL,
    merged_at TEXT,
    summary TEXT NOT NULL
);
CREATE TABLE timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class CoordinationChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset_a = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface"], 31.09, 122.09, 20, 200, 6
        )
        self.asset_b = self.service.add_asset(
            "coord1", "coordinator", "海巡02", "vessel", ["surface"], 30.6, 122.4, 20, 200, 8
        )
        self.area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-100", "surface", 31.1, 122.1, 8, 1
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _assignments(self, status=None):
        rows = self.service.state()["assignments"]
        if status:
            rows = [a for a in rows if a["status"] == status]
        return rows

    def test_sea_state_change_invalidates_and_replans(self):
        assigned = self.service.assign_area("coord1", "coordinator", self.area["id"], self.asset_a["id"], 1)
        self.assertEqual(3, assigned["basis"]["sea_state"])
        self.assertEqual(2, assigned["basis"]["asset_version"])

        # 海况 3→5：海巡01 仍适用，失效后按新依据重占
        incident = self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 5, 1)
        self.assertEqual(5, incident["sea_state"])
        invalidated = self._assignments("invalidated")
        self.assertEqual(1, len(invalidated))
        self.assertIn("海况 3→5", invalidated[0]["close_reason"])
        planned = self._assignments("planned")
        self.assertEqual(1, len(planned))
        self.assertEqual(5, planned[0]["basis"]["sea_state"])
        self.assertEqual(self.asset_a["id"], planned[0]["asset_id"])

        # 海况 5→7：海巡01 不再适用，重算后改派海巡02
        current = [i for i in self.service.state()["incidents"] if i["id"] == self.incident["id"]][0]
        self.service.update_sea_state("op1", "operator", self.incident["id"], 7, current["version"])
        planned = self._assignments("planned")
        self.assertEqual(1, len(planned))
        self.assertEqual(self.asset_b["id"], planned[0]["asset_id"])
        self.assertEqual(7, planned[0]["basis"]["sea_state"])
        assets = {a["name"]: a["status"] for a in self.service.state()["assets"]}
        self.assertEqual("available", assets["海巡01"])
        self.assertEqual("assigned", assets["海巡02"])

    def test_departed_assignment_keeps_basis(self):
        assigned = self.service.assign_area("coord1", "coordinator", self.area["id"], self.asset_a["id"], 1)
        departed = self.service.depart_assignment("op1", "operator", assigned["assignment_id"])
        self.assertEqual("departed", departed["status"])

        current = [i for i in self.service.state()["incidents"] if i["id"] == self.incident["id"]][0]
        self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 9, current["version"])
        asset_now = [a for a in self.service.state()["assets"] if a["id"] == self.asset_a["id"]][0]
        self.service.update_asset("coord1", "coordinator", self.asset_a["id"], asset_now["version"],
                                  latitude=33.0, longitude=125.0)

        still = [a for a in self._assignments("departed")][0]
        self.assertEqual(3, still["basis"]["sea_state"])
        self.assertEqual(31.09, still["basis"]["asset_latitude"])
        self.assertEqual(0, len(self._assignments("invalidated")))
        area = self.service.state()["search_areas"][0]
        self.assertEqual("active", area["status"])
        self.assertEqual(self.asset_a["id"], area["assigned_asset_id"])

    def test_concurrent_assign_first_wins_and_loser_keeps_candidate(self):
        area2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-101", "surface", 31.1, 122.1, 5, 2
        )
        barrier = threading.Barrier(2)
        results = {}

        def worker(name, area_id):
            barrier.wait()
            try:
                results[name] = ("ok", self.service.assign_area(name, "coordinator", area_id, self.asset_a["id"], 1))
            except DomainError as exc:
                results[name] = ("err", exc)

        threads = [
            threading.Thread(target=worker, args=("duty-a", self.area["id"])),
            threading.Thread(target=worker, args=("duty-b", area2["id"])),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        outcomes = sorted(value[0] for value in results.values())
        self.assertEqual(["err", "ok"], outcomes)
        loser = [value for value in results.values() if value[0] == "err"][0][1]
        self.assertEqual(409, loser.status)
        self.assertTrue(loser.details["conflicts"])
        self.assertIsNotNone(loser.details["candidate_id"])

        candidates = self.service.state()["candidates"]
        self.assertEqual(1, len(candidates))
        self.assertEqual("pending", candidates[0]["status"])
        self.assertTrue(any("资源" in item for item in candidates[0]["conflicts"]))
        self.assertEqual(1, len(self._assignments("planned")))

        # 候选在资源释放后可以重试生效
        asset_now = [a for a in self.service.state()["assets"] if a["id"] == self.asset_a["id"]][0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset_a["id"], "让位", asset_now["version"])
        promoted = self.service.retry_candidate("coord1", "coordinator", candidates[0]["id"])
        self.assertEqual("coord1", promoted["basis"]["decided_by"])
        self.assertEqual("promoted", self.service.state()["candidates"][0]["status"])
        planned = self._assignments("planned")
        self.assertEqual(1, len(planned))
        self.assertEqual(candidates[0]["area_id"], planned[0]["area_id"])

    def test_offline_assignment_idempotent_and_batch_recovery(self):
        area2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-102", "surface", 31.1, 122.1, 5, 2
        )
        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-a",
            [{"type": "assignment", "client_event_id": "ev-a1", "area_id": self.area["id"], "asset_id": self.asset_a["id"]},
             {"type": "clue", "client_event_id": "ev-c1", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.8, "source": "radio"}],
        )
        self.assertEqual(2, batch["summary"]["accepted"])
        self.assertEqual(1, len(self._assignments("planned")))

        # 同批次重放：幂等，不重复占用
        replay = self.service.merge_offline_batch("field1", "field", "batch-a", [])
        self.assertTrue(replay["idempotent"])
        # 不同批次携带同一事件号：按事件号合并，仍不重复占用
        other = self.service.merge_offline_batch(
            "field1", "field", "batch-b",
            [{"type": "assignment", "client_event_id": "ev-a1", "area_id": self.area["id"], "asset_id": self.asset_a["id"]}],
        )
        self.assertTrue(other["summary"]["events"][0]["idempotent"])
        self.assertEqual(1, len(self._assignments("planned")))

        # 模拟写入失败：批次已登记但未合并，重连后从完整批次恢复
        payload = [{"type": "assignment", "client_event_id": "ev-a2", "area_id": area2["id"], "asset_id": self.asset_b["id"]}]
        with self.service.connect() as conn:
            conn.execute(
                "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,summary,payload) VALUES(?,?,?,?,?,?)",
                ("batch-c", "field1", "failed", utcnow(), "{}", json.dumps(payload, ensure_ascii=False)),
            )
        recovered = self.service.merge_offline_batch("field1", "field", "batch-c", [])
        self.assertFalse(recovered["idempotent"])
        self.assertEqual(1, recovered["summary"]["accepted"])
        self.assertEqual(2, len(self._assignments("planned")))
        again = self.service.merge_offline_batch("field1", "field", "batch-c", [])
        self.assertTrue(again["idempotent"])
        self.assertEqual(2, len(self._assignments("planned")))

    def test_export_state_and_timeline_share_same_basis(self):
        assigned = self.service.assign_area("coord1", "coordinator", self.area["id"], self.asset_a["id"], 1)
        state = self.service.state()
        exported = self.service.export_state()
        self.assertEqual(state["assignments"], exported["assignments"])
        basis = exported["assignments"][0]["basis"]
        self.assertEqual(assigned["basis"], basis)
        timeline = self.service.incident_timeline(self.incident["id"])
        assigned_entries = [t for t in timeline if t["action"] == "area.assigned"]
        self.assertEqual(1, len(assigned_entries))
        details = json.loads(assigned_entries[0]["details"])
        self.assertEqual(basis, details["basis"])

    def test_legacy_data_backfilled_on_open(self):
        db_path = Path(self.tmp.name) / "legacy.db"
        now = utcnow()
        conn = sqlite3.connect(str(db_path))
        conn.executescript(OLD_SCHEMA)
        conn.execute(
            """INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,lead_org,
               version,created_by,created_at,updated_at) VALUES('SAR-OLD','老船',31.0,122.0,10,3,'coordinating','东海中心',4,'op',?,?)""",
            (now, now),
        )
        conn.execute(
            """INSERT INTO assets(name,kind,capabilities,status,latitude,longitude,speed_kn,range_km,max_sea_state,version,updated_at)
               VALUES('老资源','vessel','["surface"]','assigned',31.0,122.0,20,200,6,3,?)""",
            (now,),
        )
        conn.execute(
            """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,status,
               assigned_asset_id,version,created_at,updated_at) VALUES(1,'A-OLD','surface',31.1,122.1,8,1,'assigned',1,2,?,?)""",
            (now, now),
        )
        conn.execute(
            """INSERT INTO clues(incident_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,recorded_at) VALUES(1,'ev-old',31.1,122.1,0.5,'radio','unverified',5.0,'op',?)""",
            (now,),
        )
        conn.commit()
        conn.close()

        service = MaritimeSARService(db_path)
        state = service.state()
        # 旧线索缺版本 → 回填为 1
        self.assertEqual(1, state["clues"][0]["version"])
        # 历史占用 → 补建派遣任务与依据
        assignments = state["assignments"]
        self.assertEqual(1, len(assignments))
        self.assertEqual(1, assignments[0]["backfilled"])
        self.assertEqual("planned", assignments[0]["status"])
        self.assertEqual(3, assignments[0]["basis"]["asset_version"])
        self.assertEqual(2, assignments[0]["basis"]["area_version"])
        # 批次表补齐 payload 列后可正常合并恢复
        batch = service.merge_offline_batch(
            "op", "operator", "batch-old",
            [{"type": "clue", "client_event_id": "ev-old-2", "incident_id": 1,
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.6, "source": "radio"}],
        )
        self.assertEqual(1, batch["summary"]["accepted"])


if __name__ == "__main__":
    unittest.main()
