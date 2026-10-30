"""Weather ETL pipeline: extract -> transform -> quality gate -> load -> publish.

Source : Open-Meteo API (free, no key)      Warehouse: SQLite (warehouse/weather.db)
Output : reports/*.json (run report, history, data snapshots) read by the monitor site.
Env    : INJECT_FAULT=none|extract|bad_data (demo)   MOCK=1 (offline fake data)
"""
import json, os, random, sqlite3, sys, time, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).parent
DB = ROOT / "warehouse" / "weather.db"
REP = ROOT / "reports"
API = ("https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&timezone=UTC"
       "&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,wind_speed_10m,weather_code")
FAULT = os.getenv("INJECT_FAULT", "none")
MOCK = os.getenv("MOCK") == "1"
utcnow = lambda: datetime.now(timezone.utc)


def condition(code):
    for lim, name in [(0, "Clear"), (2, "Partly cloudy"), (3, "Overcast"), (48, "Fog"), (57, "Drizzle"),
                      (67, "Rain"), (77, "Snow"), (82, "Showers"), (99, "Thunderstorm")]:
        if code is not None and code <= lim:
            return name
    return "Unknown"


def comfort(f):demo
    if f is None: return None
    return "Cool" if f < 18 else "Pleasant" if f < 27 else "Warm" if f < 33 else "Hot" if f < 40 else "Extreme"


# ---------- Extract ----------
def fetch(city):
    if FAULT == "extract":
        raise RuntimeError("Injected fault: upstream API returned HTTP 503")
    if MOCK:
        r = random.Random(city["name"] + utcnow().strftime("%Y%m%d%H"))
        t = round(r.uniform(22, 34), 1)
        return {"time": utcnow().strftime("%Y-%m-%dT%H:00"), "temperature_2m": t, "apparent_temperature": t + 2,
                "relative_humidity_2m": r.randint(40, 90), "precipitation": 0.0,
                "wind_speed_10m": round(r.uniform(2, 25), 1), "weather_code": r.choice([0, 2, 3, 61])}
    last = None
    for i in range(3):
        try:
            with urllib.request.urlopen(API.format(**city), timeout=15) as resp:
                return json.load(resp)["current"]
        except Exception as e:
            last = e; time.sleep(2 * (i + 1))
    raise RuntimeError(f"API failed for {city['name']}: {last}")


# ---------- Transform ----------
def transform(raw, cities):
    rows = []
    for c in cities:
        r = raw[c["name"]]
        rows.append({"city": c["name"], "observed_at": r["time"] + "Z", "temp_c": r.get("temperature_2m"),
                     "feels_like_c": r.get("apparent_temperature"), "humidity_pct": r.get("relative_humidity_2m"),
                     "precip_mm": r.get("precipitation"), "wind_kmh": r.get("wind_speed_10m"),
                     "condition": condition(r.get("weather_code")), "comfort": comfort(r.get("apparent_temperature"))})
    if FAULT == "bad_data":
        rows[0]["temp_c"] = 999.0   # injected corrupt value -> must be caught by the quality gate
    return rows


# ---------- Data quality ----------
def quality(rows, n_expected, max_age_h=3):
    checks = []
    add = lambda name, ok, detail: checks.append({"check": name, "passed": bool(ok), "detail": detail})
    add("row_count", len(rows) == n_expected, f"{len(rows)} rows, expected {n_expected}")
    for f in ["temp_c", "feels_like_c", "humidity_pct", "precip_mm", "wind_kmh", "condition"]:
        nulls = sum(r[f] is None for r in rows)
        add(f"not_null:{f}", nulls == 0, f"{nulls} null value(s)")
    ok = lambda f, lo, hi: [r["city"] for r in rows if r[f] is not None and not lo <= r[f] <= hi]
    for f, lo, hi in [("temp_c", -20, 60), ("humidity_pct", 0, 100), ("wind_kmh", 0, 200), ("precip_mm", 0, 500)]:
        bad = ok(f, lo, hi)
        add(f"range:{f}", not bad, f"valid {lo}..{hi}" + (f"; out of range: {', '.join(bad)}" if bad else ""))
    keys = {(r["city"], r["observed_at"]) for r in rows}
    add("unique_key", len(keys) == len(rows), "city + observed_at is unique")
    try:
        age = utcnow() - datetime.strptime(rows[0]["observed_at"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
        add("freshness", age <= timedelta(hours=max_age_h), f"newest reading is {int(age.total_seconds() // 60)} min old")
    except Exception as e:
        add("freshness", False, f"cannot parse timestamp: {e}")
    return checks


# ---------- Load ----------
def load(rows, cities, db_path=DB):
    db_path.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript("""
      CREATE TABLE IF NOT EXISTS dim_city (city_id INTEGER PRIMARY KEY, name TEXT UNIQUE, lat REAL, lon REAL);
      CREATE TABLE IF NOT EXISTS fact_weather (
        city_id INTEGER, observed_at TEXT, temp_c REAL, feels_like_c REAL, humidity_pct REAL, precip_mm REAL,
        wind_kmh REAL, condition TEXT, comfort TEXT, ingested_at TEXT, PRIMARY KEY (city_id, observed_at));""")
    for c in cities:
        con.execute("INSERT OR IGNORE INTO dim_city(name, lat, lon) VALUES (?,?,?)", (c["name"], c["lat"], c["lon"]))
    ids = dict(con.execute("SELECT name, city_id FROM dim_city"))
    for r in rows:   # upsert => re-running the same hour never duplicates data (idempotent)
        con.execute("INSERT OR REPLACE INTO fact_weather VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (ids[r["city"]], r["observed_at"], r["temp_c"], r["feels_like_c"], r["humidity_pct"],
                     r["precip_mm"], r["wind_kmh"], r["condition"], r["comfort"], utcnow().isoformat(timespec="seconds")))
    con.commit()
    total = con.execute("SELECT COUNT(*) FROM fact_weather").fetchone()[0]
    con.close()
    return total


# ---------- Publish (JSON for the dashboard) ----------
def publish(db_path=DB, rep=REP):
    rep.mkdir(exist_ok=True)
    con = sqlite3.connect(db_path); con.row_factory = sqlite3.Row
    latest, recent = [], {}
    for c in con.execute("SELECT * FROM dim_city ORDER BY name").fetchall():
        rs = [dict(r) for r in con.execute(
            "SELECT observed_at, temp_c, feels_like_c, humidity_pct, precip_mm, wind_kmh, condition, comfort "
            "FROM fact_weather WHERE city_id=? ORDER BY observed_at DESC LIMIT 48", (c["city_id"],))][::-1]
        if rs:
            latest.append({"city": c["name"], **rs[-1]})
            recent[c["name"]] = [{"t": r["observed_at"], "temp_c": r["temp_c"]} for r in rs]
    con.close()
    stamp = utcnow().isoformat(timespec="seconds")
    (rep / "latest_data.json").write_text(json.dumps({"generated_at": stamp, "cities": latest}, indent=1))
    (rep / "weather_recent.json").write_text(json.dumps({"generated_at": stamp, "series": recent}))
    return len(latest)


# ---------- Orchestration ----------
def main():
    cities = json.loads((ROOT / "cities.json").read_text())
    repo, rid = os.getenv("GITHUB_REPOSITORY", "local"), os.getenv("GITHUB_RUN_ID", "local")
    report = {"run_id": rid, "run_url": f"https://github.com/{repo}/actions/runs/{rid}",
              "trigger": os.getenv("GITHUB_EVENT_NAME", "local"), "fault_injected": FAULT,
              "started_at": utcnow().isoformat(timespec="seconds"), "stages": [], "dq": [], "error": None}
    ctx = {}

    def s_extract():
        ctx["raw"] = {c["name"]: fetch(c) for c in cities}
        return {"rows_out": len(ctx["raw"]), "detail": f"{len(cities)} API calls"}

    def s_transform():
        ctx["rows"] = transform(ctx["raw"], cities)
        return {"rows_in": len(ctx["raw"]), "rows_out": len(ctx["rows"])}

    def s_quality():
        report["dq"] = quality(ctx["rows"], len(cities))
        bad = [c for c in report["dq"] if not c["passed"]]
        if bad:
            raise RuntimeError(f"{len(bad)} data quality check(s) failed: " + ", ".join(c["check"] for c in bad))
        return {"rows_in": len(ctx["rows"]), "rows_out": len(ctx["rows"]), "detail": f"{len(report['dq'])} checks passed"}

    def s_load():
        report["fact_rows"] = load(ctx["rows"], cities)
        report["rows_loaded"] = len(ctx["rows"])
        return {"rows_in": len(ctx["rows"]), "rows_out": len(ctx["rows"]), "detail": f"warehouse now {report['fact_rows']} rows"}

    def s_publish():
        return {"detail": f"{publish()} city snapshots exported"}

    failed = False
    for name, fn in [("extract", s_extract), ("transform", s_transform), ("quality", s_quality),
                     ("load", s_load), ("publish", s_publish)]:
        if failed:
            report["stages"].append({"name": name, "status": "skipped"}); continue
        t0, st = time.time(), {"name": name}
        try:
            st.update(fn()); st["status"] = "success"
        except Exception as e:
            st.update(status="failed", detail=str(e)); report["error"] = f"{name}: {e}"; failed = True
        st["duration_s"] = round(time.time() - t0, 2)
        report["stages"].append(st)
        print(f"[{st['status'].upper():7}] {name:<9} {st.get('detail', '')}")

    report["status"] = "failed" if failed else "success"
    report["finished_at"] = utcnow().isoformat(timespec="seconds")
    report["duration_s"] = round(sum(s.get("duration_s", 0) for s in report["stages"]), 2)
    REP.mkdir(exist_ok=True)
    (REP / "latest_run.json").write_text(json.dumps(report, indent=1))
    hp = REP / "run_history.json"
    hist = json.loads(hp.read_text()) if hp.exists() else []
    hp.write_text(json.dumps((hist + [report])[-100:], indent=1))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
