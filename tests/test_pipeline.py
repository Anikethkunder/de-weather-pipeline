import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pipeline as p

CITIES = [{"name": "A", "lat": 1, "lon": 2}, {"name": "B", "lat": 3, "lon": 4}]
NOW = p.utcnow().strftime("%Y-%m-%dT%H:00")
RAW = {c["name"]: {"time": NOW, "temperature_2m": 30.0, "apparent_temperature": 34.0, "relative_humidity_2m": 70,
                   "precipitation": 0.0, "wind_speed_10m": 10.0, "weather_code": 61} for c in CITIES}


class T(unittest.TestCase):
    def test_transform(self):
        rows = p.transform(RAW, CITIES)
        self.assertEqual(rows[0]["condition"], "Rain")
        self.assertEqual(rows[0]["comfort"], "Hot")

    def test_quality_passes_on_good_data(self):
        self.assertTrue(all(c["passed"] for c in p.quality(p.transform(RAW, CITIES), 2)))

    def test_quality_catches_bad_temperature(self):
        rows = p.transform(RAW, CITIES); rows[0]["temp_c"] = 999
        failed = [c["check"] for c in p.quality(rows, 2) if not c["passed"]]
        self.assertIn("range:temp_c", failed)

    def test_quality_catches_nulls(self):
        rows = p.transform(RAW, CITIES); rows[1]["humidity_pct"] = None
        self.assertFalse(all(c["passed"] for c in p.quality(rows, 2)))

    def test_load_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "t.db"; rows = p.transform(RAW, CITIES)
            self.assertEqual(p.load(rows, CITIES, db), 2)
            self.assertEqual(p.load(rows, CITIES, db), 2)   # same data again -> still 2


if __name__ == "__main__":
    unittest.main()
