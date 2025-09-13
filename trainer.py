MET_VALUES = {
    "ходьба": 3.5,
    "легкий бег": 7.0,
    "бег": 9.8,
    "плавание": 6.0,
    "велотренажер": 7.0,
    "присед": 5.0,
    "отжим": 4.0,
    "планка": 3.3,
    "йога": 2.5,
}

import re
from typing import Dict


def _estimate_exercise_minutes(line: str) -> int:
    """Attempt to estimate duration in minutes from a line of text."""
    m = re.search(r"(\d+)\s*мин", line)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*[x×]\s*(\d+)", line)
    if m:
        sets, reps = int(m.group(1)), int(m.group(2))
        return max(sets * 2, 5)
    return 10


def get_weekly_training_kcal(plan: str, weight_kg: float = 70.0) -> int:
    """Rudimentary calorie estimation for a weekly training plan.

    The function parses the plan line-by-line, matches known exercises and
    multiplies their MET value by the user's weight and estimated duration.
    """
    total = 0.0
    for line in plan.splitlines():
        for name, met in MET_VALUES.items():
            if name.lower() in line.lower():
                mins = _estimate_exercise_minutes(line)
                total += met * weight_kg * mins / 60.0
                break
    return int(total)
