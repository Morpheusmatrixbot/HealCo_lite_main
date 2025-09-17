import asyncio
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main


class DummyResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload

    def json(self):
        return self._payload


def test_search_google_portion_ml_retained(monkeypatch):
    monkeypatch.setattr(main, "GOOGLE_CSE_KEY", "test-key")
    monkeypatch.setattr(main, "GOOGLE_CSE_CX", "test-cx")
    monkeypatch.setattr(main, "is_branded_product", lambda query: False)

    async def fake_translate_clean_query(clean_query: str):
        return clean_query, clean_query

    monkeypatch.setattr(main, "translate_clean_query", fake_translate_clean_query)

    def fake_requests_get(url, params=None, timeout=None):
        return DummyResponse(
            {
                "items": [
                    {
                        "title": "Sample Juice",
                        "snippet": (
                            "Sample juice provides 40 kcal per 100 g, protein 1 g, "
                            "fat 0 g, carbs 9 g."
                        ),
                    }
                ]
            }
        )

    monkeypatch.setattr(main.requests, "get", fake_requests_get)

    async def run_search():
        return await main.search_google_for_product("sample juice", ml=250)

    result = asyncio.run(run_search())

    assert result is not None
    assert result.get("portion_ml") == 250

    portion_line = (
        f"⚖️ Порция: {int(result.get('portion_g') or 0)} г"
        if result.get("portion_g")
        else f"⚖️ Порция: {int(result.get('portion_ml') or 0)} мл"
    )

    assert portion_line == "⚖️ Порция: 250 мл"
