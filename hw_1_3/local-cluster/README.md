# ClickHouse: локальный кластер и Jupyter

Кластер `homework_cluster`: шард 01 — `ch11`, `ch12`; шард 02 —
`ch21`, `ch22`. В каждом шарде две реплики. Один Keeper обеспечивает
координацию репликации и выполнение `ON CLUSTER`. Это учебная конфигурация:
единственный Keeper не обеспечивает отказоустойчивость координации.

Для задания 4 в `cluster.xml` также описан `homework_dedup_cluster`:
два шарда, по одному узлу (`ch11`, `ch21`). Он использует те же контейнеры,
но не считает `ch12` и `ch22` репликами обычного `ReplacingMergeTree`.
В этом задании `hw.push_events_dedup` распределяет строки по
`cityHash64(event_id)`, а локальные таблицы используют `ORDER BY event_id`
и `PARTITION BY event_id % 16`. Все версии события остаются на одном шарде
и в одной партиции. Источник `homework_data.push_events_source` создаётся
ячейками задания 4 на `ch11` по `generator.sql`, без удаления базы.

## Запуск

Запустите Docker Desktop в режиме Linux containers. Затем в PowerShell:

```powershell
cd C:\Users\dshalimov\Projects\vk-technopark-dwh-internship\hw_1_3\local-cluster
docker compose up -d --build --wait --wait-timeout 300
docker compose ps
```

При первом запуске Docker скачает образы и соберёт Jupyter с Python-зависимостями.
Jupyter запускается после того, как все четыре сервера пройдут healthcheck.

Откройте <http://localhost:8890/lab>, токен входа: `homework`.

- `seminar/clickhouse_practice.ipynb` — адаптированный ноутбук семинара.
- `hw_1_3/hw_1_3.ipynb` — заготовка для домашки: подключение, проверка кластера
  и пустая ячейка для решения. Выполняйте ячейки сверху вниз.

Каталог `notebooks` смонтирован в `/home/jovyan/work`: изменения в Jupyter
сохраняются в файлах проекта. Исходный `clickhouse_practice.ipynb` в корне
оставлен без изменений; его старые настройки подключения не подходят этому кластеру.
Рабочая копия очищена от старых результатов. Повторное выполнение INSERT
в семинаре добавляет данные повторно.

## Подключение

Jupyter — удобный клиент для Python, но не обязательная часть ClickHouse.
Можно также пользоваться `clickhouse-client`, HTTP или ноутбуком в IDE.

| Откуда | HTTP-адрес | Пользователь | Пароль |
| --- | --- | --- | --- |
| Из Jupyter в Compose | `ch11:8123` | `default` | `homework` |
| Из Python/IDE на компьютере | `localhost:8123` | `default` | `homework` |

`clickhouse-connect` использует HTTP. Порт `9000` нужен для native-протокола
(`clickhouse-client`), не для этого Python-драйвера. Внутри контейнера
`localhost` означает сам контейнер, поэтому используется имя сервиса `ch11`.
Ноутбуки читают параметры из переменных окружения Compose; без них используют
`localhost:8123` и тот же пароль. Для локального Python установите зависимости:

```powershell
python -m pip install -r requirements.txt
```

HTTP-порты на компьютере: `ch11` — 8123, `ch12` — 8124, `ch21` — 8125,
`ch22` — 8126. Внутри Docker все узлы используют HTTP 8123 и native 9000.
Все опубликованные порты привязаны к `127.0.0.1`.

## Проверка кластера без Jupyter

```powershell
docker compose exec ch11 clickhouse-client --password homework --query "SELECT hostName() AS host, version() FROM clusterAllReplicas('homework_cluster', system.one) ORDER BY host"
```

Ожидаются четыре строки: `ch11`, `ch12`, `ch21`, `ch22`.
Подключение из Jupyter проверьте первыми ячейками любого из подготовленных
ноутбуков: запрос через `clusterAllReplicas` должен вернуть те же четыре узла.

Базы и таблицы семинара создаются его ячейками, поэтому отдельный `init`
не нужен. Старый сервис `init` удалён: его `init.sh` и каталог `sql` отсутствовали.
`generator.sql` не запускается автоматически.

## Остановка и диагностика

```powershell
docker compose logs --tail 100 keeper ch11 jupyter
docker compose down
```

`down` сохраняет данные ClickHouse в именованных volumes. Команда
`docker compose down -v` удаляет данные кластера; используйте её только
для намеренного полного сброса. Файлы ноутбуков при этом сохраняются.

Текущая настройка `insert_quorum=2` требует обе живые реплики шарда для
вставки в ReplicatedMergeTree. Не останавливайте реплики во время обычного
выполнения семинара. При нехватке памяти проверьте лимит Docker Desktop:
настроенные в XML лимиты серверов не резервируют память и не ограничивают
суммарное потребление всех контейнеров.
