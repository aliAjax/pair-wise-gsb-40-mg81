import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib import request
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiHandler, DomainError, MaritimeSARService  # noqa: E402


class MaritimeSARFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-001", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_complete_assignment_clue_offline_and_close_flow(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-01", "surface", 31.1, 122.1, 8, 1
        )
        assigned = self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        self.assertEqual("assigned", assigned["status"])
        clue = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-1", 31.1, 122.1, 0.9, "visual", area["id"]
        )
        self.assertEqual("verified", self.service.verify_clue("analyst1", "analyst", clue["id"], "verified")["status"])
        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-1",
            [{"type": "clue", "client_event_id": "off-1", "incident_id": self.incident["id"],
              "latitude": 31.11, "longitude": 122.11, "confidence": 0.7, "source": "radio"}],
        )
        self.assertEqual(1, batch["summary"]["accepted"])
        self.assertTrue(self.service.merge_offline_batch("field1", "field", "batch-1", [])["idempotent"])
        updated_asset = self.service.list_assets()[0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "任务移交", updated_asset["version"])
        current_area = self.service.state()["search_areas"][0]
        self.service.complete_area("coord1", "coordinator", area["id"], "abandoned", current_area["version"])
        current_incident = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", current_incident["version"])
        self.assertEqual("closed", closed["status"])
        self.assertGreaterEqual(len(self.service.incident_timeline(self.incident["id"])), 6)

    def test_duplicate_alarm_and_invalid_position_are_controlled(self):
        duplicate = self.service.create_incident(
            "op1", "operator", "SAR-002", "海燕号", 31.01, 122.01, 5.0, 3, "东海中心"
        )
        self.assertEqual("duplicate", duplicate["status"])
        self.assertEqual(self.incident["id"], duplicate["duplicate_of"])
        invalid = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-far", 45.0, 130.0, 0.8, "radio"
        )
        self.assertEqual("invalid", invalid["status"])
        with self.assertRaises(DomainError):
            self.service.verify_clue("field1", "field", invalid["id"], "verified")

    def test_assignment_conflict_and_permission(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-02", "surface", 31.1, 122.1, 5
        )
        self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        area2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-03", "surface", 31.2, 122.2, 5
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.assign_area("coord1", "coordinator", area2["id"], self.asset["id"], self.asset["version"])
        self.assertEqual(409, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.service.create_search_area("field1", "field", self.incident["id"], "A-04", "surface", 31, 122, 5)
        self.assertEqual(403, ctx2.exception.status)


def _post(service, path, payload, user="c1", role="coordinator", port=8299):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request("http://127.0.0.1:%d%s" % (port, path), data=data,
                          headers={"Content-Type": "application/json", "X-User": user, "X-Role": role},
                          method="POST")
    try:
        with request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except Exception as exc:  # HTTPError
        body = json.loads(exc.read())
        return exc.code, body


class CoordinationChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "chain.db"
        self.service = MaritimeSARService(self.db)
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-100", "长风号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.near = self.service.add_asset(
            "coord1", "coordinator", "近程艇", "vessel", ["surface"], 31.02, 122.0, 18, 120, 6
        )
        self.far = self.service.add_asset(
            "coord1", "coordinator", "远程船", "vessel", ["surface"], 30.5, 121.5, 20, 300, 6
        )
        self.p1 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "P-1", "surface", 31.1, 122.1, 6, 1
        )
        self.p2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "P-2", "surface", 31.3, 122.3, 6, 2
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _live(self):
        return {a["area_id"]: a for a in self.service.state()["assignments"]
                if a["phase"] in ("reserved", "active")}

    def test_reserve_with_basis_dispatch_keeps_basis_when_world_changes(self):
        res = self.service.assign_area("coord1", "coordinator", self.p1["id"], self.near["id"],
                                       self.near["version"], self.p1["version"])
        self.assertEqual("assigned", res["status"])
        chain = self.service.state()["assignments"]
        live = [a for a in chain if a["phase"] == "reserved"]
        self.assertEqual(1, len(live))
        rec = live[0]
        self.assertEqual(self.incident["version"], rec["incident_version"])
        self.assertEqual(self.near["version"], rec["asset_version"])
        self.assertEqual(self.p1["version"], rec["area_version"])
        self.assertTrue(rec["basis"]["checks"]["range_ok"])
        self.assertTrue(rec["basis"]["checks"]["sea_state_ok"])

        dispatched = self.service.dispatch_assignment("coord1", "coordinator", self.p1["id"],
                                                      self.p1["version"] + 1)
        self.assertEqual("active", dispatched["phase"])

        # 海况变化 + 资源位置漂移到航程外：已出发任务保留原依据
        self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 7,
                                      self.incident["version"])
        near = [a for a in self.service.list_assets() if a["id"] == self.near["id"]][0]
        moved = self.service.update_asset("coord1", "coordinator", self.near["id"], near["version"],
                                          latitude=40.0, longitude=140.0)
        kept = self._live().get(self.p1["id"])
        self.assertIsNotNone(kept)
        self.assertEqual("active", kept["phase"])
        self.assertEqual(rec["incident_version"], kept["incident_version"])
        self.assertEqual(rec["asset_version"], kept["asset_version"])
        self.assertEqual(rec["area_version"], kept["area_version"])
        self.assertEqual(kept["departed_at"], dispatched["departed_at"])
        # 漂移后资源不再可用作重算（被已出发任务占用），p2 未满足
        self.assertNotIn(self.p2["id"], self._live())

    def test_sea_state_change_invalidates_reserved_and_rebooks(self):
        self.service.assign_area("coord1", "coordinator", self.p1["id"], self.near["id"],
                                 self.near["version"], self.p1["version"])
        # 海况升到 7：近程艇 max 6 失效；远程船 max 6 同样超限，两个未预占区域都 unmet
        result = self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 7,
                                               self.incident["version"])["recompute"]
        self.assertEqual(1, len(result["invalidated"]))
        self.assertEqual(0, len(result["rebooked"]))
        self.assertEqual(2, len(result["unmet"]))
        chain = self.service.state()["assignments"]
        self.assertEqual("invalidated", [a for a in chain if a["area_id"] == self.p1["id"]][0]["phase"])
        area1 = [a for a in self.service.state()["search_areas"] if a["id"] == self.p1["id"]][0]
        self.assertEqual("planned", area1["status"])
        self.assertIsNone(area1["assigned_asset_id"])
        near = [a for a in self.service.list_assets() if a["id"] == self.near["id"]][0]
        self.assertEqual("available", near["status"])
        # 海况恢复后重新按优先级预占：近程艇最近
        self.service.update_sea_state("coord1", "coordinator", self.incident["id"], 3,
                                      self.incident["version"] + 1)
        live = self._live()
        self.assertIn(self.p1["id"], live)
        self.assertEqual("reserved", live[self.p1["id"]]["phase"])
        self.assertEqual(self.near["id"], live[self.p1["id"]]["asset_id"])

    def test_concurrent_reservation_first_writer_wins_later_kept_as_candidate(self):
        # 两个值班员同时把同一资源预占到同一区域：先到者生效，后到者保留候选
        a3 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "P-3", "surface", 31.25, 122.25, 6, 3
        )
        errors = []

        def call(area_id):
            try:
                self.service.assign_area("coord1", "coordinator", area_id, self.near["id"],
                                         self.near["version"])
            except DomainError as exc:
                errors.append((area_id, exc))

        t1 = threading.Thread(target=call, args=(a3["id"],))
        t2 = threading.Thread(target=call, args=(a3["id"],))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(1, len(errors))
        loser_area, exc = errors[0]
        self.assertEqual(a3["id"], loser_area)
        self.assertEqual(409, exc.status)
        self.assertIn("candidate_id", exc.details)
        candidate_id = exc.details["candidate_id"]
        self.assertIn("area_taken", {c["code"] for c in exc.details["conflicts"]})

        state = self.service.state()
        candidate = [a for a in state["assignments"] if a["id"] == candidate_id][0]
        self.assertEqual("candidate", candidate["phase"])
        self.assertEqual(self.near["id"], candidate["asset_id"])
        # 采纳候选时资源仍忙 -> 拒绝但保留候选
        with self.assertRaises(DomainError):
            self.service.accept_candidate("coord1", "coordinator", candidate_id)
        still = [a for a in self.service.state()["assignments"] if a["id"] == candidate_id][0]
        self.assertEqual("candidate", still["phase"])
        # 先到者结束后再采纳，候选转为正式预占
        a3_area = [a for a in self.service.state()["search_areas"] if a["id"] == a3["id"]][0]
        self.service.complete_area("coord1", "coordinator", a3["id"], "completed", a3_area["version"])
        accepted = self.service.accept_candidate("coord1", "coordinator", candidate_id)
        self.assertEqual("reserved", accepted["phase"])

    def test_stale_version_queues_candidate_with_conflict(self):
        self.service.assign_area("coord1", "coordinator", self.p1["id"], self.near["id"],
                                 self.near["version"], self.p1["version"])
        self.service.complete_area(
            "coord1", "coordinator", self.p1["id"], "completed", self.p1["version"] + 1
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.assign_area("coord1", "coordinator", self.p2["id"], self.near["id"],
                                     expected_asset_version=self.near["version"])  # 旧版本
        self.assertEqual(409, ctx.exception.status)
        codes = {c["code"] for c in ctx.exception.details["conflicts"]}
        self.assertIn("version_stale", codes)
        candidate = [a for a in self.service.state()["assignments"]
                     if a["id"] == ctx.exception.details["candidate_id"]][0]
        self.assertEqual("candidate", candidate["phase"])

    def test_plan_respects_priority_and_recomputes_after_area_change(self):
        # 独立事件：远程船停在 a1 中心附近，近程艇停在 a2 中心附近
        inc = self.service.create_incident(
            "coord1", "coordinator", "SAR-101", "远洋号", 31.0, 122.0, 10.0, 3, "东海中心")
        a1 = self.service.create_search_area(
            "coord1", "coordinator", inc["id"], "Q-1", "surface", 30.5, 121.5, 6, 1)
        a2 = self.service.create_search_area(
            "coord1", "coordinator", inc["id"], "Q-2", "surface", 31.3, 122.3, 6, 2)
        plan = self.service.plan_incident("coord1", "coordinator", inc["id"])
        by_area = {o["area_id"]: o for o in plan["outcomes"]}
        self.assertEqual(self.far["id"], by_area[a1["id"]]["asset_id"])
        self.assertEqual(self.near["id"], by_area[a2["id"]]["asset_id"])

        # 区域 a2 版本/优先级变化：未出发预占失效并重算；a1 的在执行预占同样经历失效-重算，依据更新为新版本
        a1_rec_before = [a for a in self.service.state()["assignments"]
                         if a["area_id"] == a1["id"] and a["phase"] == "reserved"][0]
        a2_fresh = [x for x in self.service.state()["search_areas"] if x["id"] == a2["id"]][0]
        self.service.update_area("coord1", "coordinator", a2["id"], a2_fresh["version"], priority=5)
        state = self.service.state()
        live = {a["area_id"]: a for a in state["assignments"] if a["phase"] == "reserved"}
        self.assertEqual(self.far["id"], live[a1["id"]]["asset_id"])
        self.assertEqual(self.near["id"], live[a2["id"]]["asset_id"])
        self.assertEqual(5, live[a2["id"]]["priority"])
        self.assertGreater(live[a1["id"]]["id"], a1_rec_before["id"])
        # 失效链记录保留可审计
        self.assertTrue(any(x["action"] == "area.invalidated"
                            for x in self.service.incident_timeline(inc["id"])))

    def test_offline_batch_dedup_by_event_id_and_recovery(self):
        events = [
            {"type": "clue", "client_event_id": "c-1", "incident_id": self.incident["id"],
             "latitude": 31.1, "longitude": 122.1, "confidence": 0.8, "source": "radio"},
            {"type": "assignment", "client_event_id": "a-1", "area_id": self.p1["id"],
             "asset_id": self.near["id"]},
        ]
        batch = self.service.merge_offline_batch("coord1", "coordinator", "B-9", events)
        self.assertEqual(2, batch["summary"]["accepted"])
        self.assertFalse(batch["idempotent"])
        self.assertTrue(self._live().get(self.p1["id"]))

        # 完整批次重复提交：批次幂等，不重复占用
        again = self.service.merge_offline_batch("coord1", "coordinator", "B-9", events)
        self.assertTrue(again["idempotent"])

        # 不同批次携带相同事件号：按事件号去重，不再预占/插线索
        batch2 = self.service.merge_offline_batch("coord1", "coordinator", "B-10", events)
        self.assertEqual(2, batch2["summary"]["deduplicated"])
        self.assertEqual(0, batch2["summary"]["accepted"])

        # 模拟写入失败：批次标 failed，凭同一批次号从完整载荷恢复
        self.tmp2 = tempfile.TemporaryDirectory()
        try:
            original_apply = type(self.service)._apply_offline_events
            calls = {"n": 0}

            def flaky(self_, conn, actor, role, batch_pk, evs):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("disk full")
                return original_apply(self_, conn, actor, role, batch_pk, evs)

            type(self.service)._apply_offline_events = flaky
            try:
                with self.assertRaises(DomainError) as ctx:
                    self.service.merge_offline_batch("coord1", "coordinator", "B-11", [
                        {"type": "timeline", "client_event_id": "t-1",
                         "incident_id": self.incident["id"], "action": "offline.note",
                         "details": {"text": "离线备注"}}
                    ])
                self.assertEqual(500, ctx.exception.status)
            finally:
                type(self.service)._apply_offline_events = original_apply
            recovered = self.service.merge_offline_batch("coord1", "coordinator", "B-11", [
                # 即使重试时提交内容不全，也以已落库的完整批次为准
            ])
            self.assertTrue(recovered["recovered"])
            self.assertEqual(1, recovered["summary"]["accepted"])
            # 再次恢复仍幂等
            once_more = self.service.merge_offline_batch("coord1", "coordinator", "B-11", [])
            self.assertTrue(once_more["idempotent"])
        finally:
            self.tmp2.cleanup()

    def test_offline_assignment_from_field_becomes_candidate(self):
        batch = self.service.merge_offline_batch("field9", "field", "FB-1", [
            {"type": "assignment", "client_event_id": "fa-1", "area_id": self.p1["id"],
             "asset_id": self.near["id"]},
        ])
        self.assertEqual(1, batch["summary"]["candidates"])
        self.assertEqual(0, batch["summary"]["accepted"])
        self.assertNotIn(self.p1["id"], self._live())
        candidate = [a for a in self.service.state()["assignments"] if a["phase"] == "candidate"][0]
        self.assertIn("offline_pending_confirmation",
                      {c["code"] for c in candidate["conflicts"]})

    def test_export_and_timeline_share_same_basis(self):
        self.service.assign_area("coord1", "coordinator", self.p1["id"], self.near["id"],
                                 self.near["version"], self.p1["version"])
        export = self.service.export_chain(self.incident["id"])
        state = self.service.state()
        exp_rec = [a for a in export["assignments"] if a["area_id"] == self.p1["id"]][0]
        state_rec = [a for a in state["assignments"] if a["area_id"] == self.p1["id"]][0]
        self.assertEqual(exp_rec["basis"], state_rec["basis"])
        timeline = self.service.incident_timeline(self.incident["id"])
        dispatched_basis = [t for t in timeline if t["action"] == "area.assigned"][0]
        basis = json.loads(dispatched_basis["basis"])
        self.assertEqual(exp_rec["id"], basis["assignment_id"])
        self.assertEqual(exp_rec["incident_version"], basis["incident_version"])
        self.assertEqual(exp_rec["asset_version"], basis["asset_version"])
        self.assertEqual(exp_rec["area_version"], basis["area_version"])


class BackfillTest(unittest.TestCase):
    def test_legacy_rows_get_backfilled_basis(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Path(tmp.name) / "legacy.db"
        service = MaritimeSARService(db)
        with service.connect() as conn:
            now = "2026-09-01T00:00:00+00:00"
            conn.execute(
                """INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,
                   status,lead_org,created_by,created_at,updated_at)
                   VALUES('OLD-1','旧船',31.0,122.0,10,3,'coordinating','旧中心','x',?,?)""", (now, now))
            conn.execute(
                """INSERT INTO assets(name,kind,capabilities,status,latitude,longitude,speed_kn,range_km,
                   max_sea_state,updated_at) VALUES('旧艇','vessel','["surface"]','assigned',31.0,122.0,
                   20,100,6,?)""", (now,))
            conn.execute(
                """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,
                   status,assigned_asset_id,created_at,updated_at)
                   VALUES(1,'OLD-A','surface',31.0,122.0,5,1,'assigned',1,?,?)""", (now, now))
            conn.execute(
                "INSERT INTO timeline(incident_id,actor,action,details,created_at,basis) "
                "VALUES(1,'x','area.assigned','{\"area_id\": 1}',?,'')", (now,))
        # 重新初始化触发回填
        service2 = MaritimeSARService(db)
        state = service2.state()
        rec = [a for a in state["assignments"] if a["area_id"] == 1][0]
        self.assertTrue(rec["backfilled"])
        self.assertEqual("reserved", rec["phase"])
        self.assertTrue(rec["basis"]["backfilled"])
        basis = json.loads([t for t in state["timeline"] if t["action"] == "area.assigned"][0]["basis"])
        self.assertTrue(basis["backfilled"])


class HttpChainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.service = MaritimeSARService(Path(cls.tmp.name) / "http.db")
        ApiHandler.service = cls.service
        cls.server = ThreadingHTTPServer(("127.0.0.1", 8299), ApiHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.tmp.cleanup()

    def test_conflict_response_contains_candidate(self):
        inc = self.service.create_incident("c1", "coordinator", "H-1", "船", 31.0, 122.0, 10, 3, "中心")
        asset = self.service.add_asset("c1", "coordinator", "艇", "vessel", ["surface"],
                                       31.0, 122.0, 20, 100, 6)
        a1 = self.service.create_search_area("c1", "coordinator", inc["id"], "H-A", "surface",
                                             31.1, 122.1, 5, 1)
        a2 = self.service.create_search_area("c1", "coordinator", inc["id"], "H-B", "surface",
                                             31.2, 122.2, 5, 2)
        status, _ = _post(self.service, "/api/assignments",
                          {"area_id": a1["id"], "asset_id": asset["id"],
                           "expected_asset_version": asset["version"]}, port=8299)
        self.assertEqual(201, status)
        status, body = _post(self.service, "/api/assignments",
                             {"area_id": a2["id"], "asset_id": asset["id"],
                              "expected_asset_version": asset["version"]}, port=8299)
        self.assertEqual(409, status)
        self.assertIn("candidate_id", body)
        self.assertIn("conflicts", body)
        status, body = _post(self.service, "/api/incidents/sea-state",
                             {"incident_id": inc["id"], "sea_state": 9,
                              "expected_version": inc["version"]}, port=8299)
        self.assertEqual(201, status)
        self.assertIn("recompute", body)


if __name__ == "__main__":
    unittest.main()
