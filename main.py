# === DEBUGGING PATCH FOR SEARCH SYSTEM ===

def debug_search_self_test():
    """
    Проверка системы поиска по ключевым словам и брендовым продуктам.
    Логирует и возвращает результаты поиска для 'яблоко' и 'Coca-Cola'.
    """
    import asyncio

    async def _test():
        test_cases = ["яблоко", "Coca-Cola"]
        results = {}
        for query in test_cases:
            print(f"[SELF-TEST] Запрос: '{query}'")
            try:
                res = await ai_meal_json({}, query)
                found = bool(res and res.get("kcal"))
                print(f"[SELF-TEST] {'НАЙДЕН' if found else 'НЕ найден'}: {res}")
                results[query] = found
            except Exception as e:
                print(f"[SELF-TEST] Ошибка поиска: {e}")
                results[query] = False
        return results

    # Запускать только вручную для диагностики
    # asyncio.run(_test())
    return _test

# --- Встраиваемый self-test для main ---
if __name__ == "__main__":
    # ... (ваш запуск main ниже)
    print("=== AI SEARCH SELF-TEST ===")
    try:
        import asyncio
        asyncio.run(debug_search_self_test()())
    except Exception as e:
        print(f"Self-test failed: {e}")

    main()  # запуск бота как обычно
