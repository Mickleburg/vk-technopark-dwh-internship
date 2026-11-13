"""Короткий вывод сохранённых проверок, замеров и примеров в ноутбуках."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load(task, name):
    return json.loads((ROOT / f'results/task{task}/{name}.json').read_text(encoding='utf-8'))


def show_checks(task):
    result = load(task, 'validation')
    if task == 1:
        assert result['no_duplicate_campaign_user']
        assert all(r['except_all_both_directions'] for r in result['equality'].values())
        print(f"Строк в аудитории: {result['rows']:,}")
        print('Все четыре варианта совпали. Повторов campaign_id + user_id нет.')
    elif task == 2:
        assert all(result['full_bag_equality'].values())
        assert result['rows'] == result['expected_rows']
        print(f"Строк после JOIN: {result['rows']:,}")
        print('Broadcast и Salt Join совпали с исходным результатом по всем колонкам и повторам.')
    else:
        assert all(result[k] for k in ['unique_users', 'full_sample_preserved', 'independent_reference_equal',
                                      'edge_cases_passed', 'nonnull_min_le_max'])
        print(f"Пользователей: {result['users']}")
        print('Выборка сохранена, user_uid уникален. Признаки совпали с расчётом на Python.')
        print('Проверки NULL, повторов и min ≤ max прошли.')


def show_measurements(task):
    summary = load(task, 'summary')
    for row in summary:
        print(f"{row['variant']}: {row['runs']} замеров, медиана {row['seconds_median']:.2f} с "
              f"({row['seconds_min']:.2f}–{row['seconds_max']:.2f}), файлов {row['files']}.")
        print(f"  Shuffle read/write: {row['shuffle_read_bytes']/1048576:.1f}/"
              f"{row['shuffle_write_bytes']/1048576:.1f} МиБ; "
              f"spill memory/disk: {row['memory_spill_bytes']/1048576:.1f}/"
              f"{row['disk_spill_bytes']/1048576:.1f} МиБ.")
    print(f'Все замеры: results/task{task}/measurements.csv')


def show_plans(task):
    checks = load(task, 'plan_checks')
    for check in checks:
        if check['plan'].endswith('_0.txt'):
            print(f"{check['variant']}: {', '.join(check['operators'])}; AQE = {check['aqe']}")
    print(f'Полные планы каждого замера: results/task{task}/plans/')


def show_examples():
    for row in load(3, 'examples'):
        print(f"user_uid = {row['user_uid']}, оценок с подходящими просмотрами: {row['ratings_present']}")
        for rating in [8, 9, 10]:
            lo, hi = row[f'feat_min_watched_time_rating_{rating}'], row[f'feat_max_watched_time_rating_{rating}']
            print(f"  Оценка {rating}: min = {lo if lo is not None else 'NULL'}, max = {hi if hi is not None else 'NULL'}")
