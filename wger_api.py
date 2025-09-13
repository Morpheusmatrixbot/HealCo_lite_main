import os
from typing import Any, Dict, Tuple

import httpx
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("WGER_API_TOKEN")
HEADERS = {"Authorization": f"Token {TOKEN}"} if TOKEN else {}
BASE_URL = "https://wger.de/api/v2"

_cache: Dict[Tuple[str, Tuple[Tuple[str, Any], ...]], Any] = {}

async def _get(path: str, params: Dict[str, Any] | None = None) -> Any:
    params = params or {}
    cache_key = (path, tuple(sorted(params.items())))
    if cache_key in _cache:
        return _cache[cache_key]

    async with httpx.AsyncClient(base_url=BASE_URL, headers=HEADERS) as client:
        resp = await client.get(path, params=params)
        resp.raise_for_status()
        data = resp.json()
        _cache[cache_key] = data
        return data

async def fetch_exercises(muscle_id: int, language: int = 2) -> Any:
    """Fetch exercises filtered by muscle id and language."""
    params = {"muscles": muscle_id, "language": language}
    return await _get("/exercise/", params)

async def fetch_equipment() -> Any:
    """Fetch list of equipment."""
    return await _get("/equipment/")

async def fetch_muscles() -> Any:
    """Fetch list of muscles."""
    return await _get("/muscle/")

