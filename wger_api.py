import logging
from typing import List, Dict

import httpx

logger = logging.getLogger("wger_api")

API_URL = "https://wger.de/api/v2/exerciseinfo/"

async def fetch_exercises(goal: str, inventory: str, injuries: str) -> List[Dict[str, str]]:
    """Fetch exercises from wger based on goal, inventory and injuries."""
    params = {"language": 2, "limit": 100, "status": 2}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(API_URL, params=params)
            resp.raise_for_status()
            data = resp.json().get("results", [])
    except Exception as e:
        logger.error(f"Failed to fetch exercises from wger: {e}")
        return []

    available_equipment = [i.strip().lower() for i in inventory.split(',') if i.strip()]
    exercises: List[Dict[str, str]] = []
    for ex in data:
        ex_equipment = [eq["name"].lower() for eq in ex.get("equipment", [])]
        if available_equipment and not any(eq in available_equipment or eq == 'без оборудования' for eq in ex_equipment):
            continue
        primary_muscle = ex.get("muscles", [{}])[0].get("name", "общая")
        level = ex.get("level", "средний")
        exercises.append({"name": ex["name"], "muscle": primary_muscle, "level": level})
    return exercises
