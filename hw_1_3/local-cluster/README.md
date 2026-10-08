# ClickHouse:

Кластер `homework_cluster`: шард 01 — `ch11`, `ch12`; шард 02 —
`ch21`, `ch22`. В каждом шарде две реплики. Один Keeper обеспечивает
координацию репликации и выполнение `ON CLUSTER`.

## Запуск

Запустите Docker Desktop. Затем в PowerShell:

```powershell
cd ...\vk-technopark-dwh-internship\hw_1_3\local-cluster
docker compose up -d --build --wait --wait-timeout 300
docker compose ps
```

Откройте <http://localhost:8890/lab>, токен входа: `homework`.

- `seminar/clickhouse_practice.ipynb` — адаптированный ноутбук семинара.
- `hw_1_3/hw_1_3.ipynb` — дзшка

Каталог `notebooks` смонтирован в `/home/jovyan/work`: изменения в Jupyter
сохраняются в файлах проекта.
