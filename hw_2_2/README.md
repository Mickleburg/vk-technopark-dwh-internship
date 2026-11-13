# ДЗ 2.2 — Spark

Из каталога `hw_2_2`:

```powershell
docker compose up -d --build
docker compose exec -T jupyter python prepare.py
docker compose exec -T jupyter python execute_notebooks.py
```

Jupyter: <http://localhost:8892/lab>, токен `homework`.

Планы завершённых запросов: `docker compose --profile history up -d history`,
затем <http://localhost:18082>. Event logs сохраняются в `results/events/`.

- `task1.ipynb`, `task2.ipynb`, `task3.ipynb` — решения и результаты.
- `task1.py`–`task3.py` — код; `common.py` — замеры и сбор метрик.
- `inputs/` — исходные данные; нужны все части архива и manifest.json.
- `results/` — планы, замеры, диагностика и проверки.
- `results/task1/measurements.csv`, `results/task2/measurements.csv` — все замеры.
- Входы: `s3a://hw-2-2/input/`; результаты: `s3a://hw-2-2/results/`.
- Задание 1: `input/task1/users_scd2` + `campaigns.csv` → `results/task1/{вариант}`.
- Задание 2: `input/task2/views` + `catalogue` → `results/task2/{вариант}`.
- Задание 3: CSV из `input/task3/` → `results/task3/user_features`.

Рядом должна находиться папка `hw_2_1`: использую её Dockerfile, генератор
и скрипт SCD2. `prepare.py` распаковывает входы из `inputs/` и генерирует
5 млн событий. `data/` и event logs создаются при запуске, в сдачу не входят.

Признаки: `feat_min_watched_time_rating_8`, `feat_max_watched_time_rating_8`
и аналогично для 9/10. Единицы `watched_time` сохранены.
