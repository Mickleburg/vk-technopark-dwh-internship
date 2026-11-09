# ДЗ 2.1 — SCD2

Из каталога `hw_2_1` при запущенном Docker Desktop:

```powershell
docker compose up -d --build
docker compose exec -T jupyter python execute_notebook.py
```

Jupyter: <http://localhost:8891/lab>, токен `homework`.

- `hw_2_1.ipynb` — выполненный ноутбук.
- `scd2.py` — сборка истории; `queries/` — SQL; `results/` — результаты проверок.
- Источник: `s3a://hw-2-1/source/event_log/`.
- Результат: `s3a://hw-2-1/target/users_scd2` — **322 версии из 5376 событий**.

Остановка: `docker compose down`.
