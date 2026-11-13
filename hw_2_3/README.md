# ДЗ 2.3

В DAG `hw23_user_campaigns` две задачи: построение SCD2 и расчёт получателей рассылок. Взял demo: 6 000 событий, 100 пользователей. Время везде UTC.

Запуск из этой папки:

```powershell
docker compose up -d --build
docker compose run --rm --no-deps tools python /opt/airflow/scripts/verify.py
```

`verify.py` включает DAG, ждёт три запуска за 23–25 января 2026 года, затем повторяет обе задачи за 23 января через Clear и сравнивает результаты.

Airflow: <http://localhost:8084>, `student` / `student`. RustFS: <http://localhost:9401>, `hw23student` / `hw23student-secret`. S3 API — порт 9400. Настройки — в `.env.example`.

Airflow 2.10.5, Spark 3.5.4 (`local[2]`), Hadoop 3.3.4, Java 17, PostgreSQL 16.6. Образ RustFS закрепил по digest в Compose.

Снимок строю по всей истории до конца расчётных суток. Новая версия появляется при изменении ФИО, почты или логина. Получателей ищу по домену и интервалу `[valid_from, valid_to)`. Рассылок мало, поэтому для JOIN использовал broadcast.

Входы лежат в `s3a://hw23/raw/demo/`. Результаты — в `s3a://hw23/results/demo/`: `user_scd2/snapshot_date=D/` и `campaign_recipients/calculation_date=D/`. При повторе заменяю только свою дату.

[Результаты и проверки](results/README.md), [условие](ASSIGNMENT.md). Код расчётов — `jobs/`, проверки — `scripts/verify.py`.

Остановка: `docker compose down`, данные в томах сохраняются.
