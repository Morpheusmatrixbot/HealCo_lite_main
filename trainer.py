import re
from typing import Dict

# Метаболические эквиваленты (MET) для популярных активностей
# Значения усреднены и предназначены лишь для примерной оценки
_MET_VALUES: Dict[str, float] = {
    "бег": 9.8,
    "ходь": 3.8,
    "плав": 8.0,
    "вел": 7.5,
    "силов": 6.0,
    "йога": 3.0,
    "стретч": 2.3,
    "пилат": 3.5,
    "кардио": 6.0,
}


def get_weekly_training_kcal(plan: str, weight_kg: float = 70.0) -> int:
    """Оценивает расход калорий за неделю по тексту плана.

    Функция ищет в тексте строки с упражнениями и продолжительностью
    (в минутах), подбирает MET по ключевым словам и рассчитывает
    суммарный расход калорий. Если упражнение неизвестно,
    используется MET=5.0.

    Parameters
    ----------
    plan: str
        Текст тренировочного плана.
    weight_kg: float
        Вес пользователя в килограммах (по умолчанию 70 кг).

    Returns
    -------
    int
        Оценка общего расхода калорий за неделю.
    """
    if not plan:
        return 0

    total = 0.0
    for line in plan.splitlines():
        line_lower = line.lower()
        m = re.search(r"(\d+)\s*мин", line_lower)
        if not m:
            continue
        minutes = int(m.group(1))
        met = 5.0  # значение по умолчанию
        for key, val in _MET_VALUES.items():
            if key in line_lower:
                met = val
                break
        kcal = met * weight_kg * minutes / 60.0
        total += kcal
    return int(total)
