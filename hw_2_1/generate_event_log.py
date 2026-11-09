#!/usr/bin/env python3
"""Generate reproducible CSV event logs for an SCD Type 2 assignment.

The script uses only the Python standard library and writes data incrementally,
so the requested row count is not accumulated in memory.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


UINT32_MAX = 4_294_967_295
CSV_COLUMNS = ("event_time", "user_id", "user_fio", "user_email", "user_login")

FIRST_NAMES = (
    "Aaron", "Adam", "Adrian", "Aiden", "Alexander", "Alice", "Allison",
    "Amanda", "Amelia", "Andrew", "Angela", "Anna", "Anthony", "Ashley",
    "Austin", "Benjamin", "Brandon", "Brian", "Brittany", "Brooke", "Caleb",
    "Cameron", "Caroline", "Charles", "Charlotte", "Chloe", "Christopher",
    "Claire", "Daniel", "David", "Dylan", "Edward", "Elizabeth", "Ella",
    "Emily", "Emma", "Eric", "Ethan", "Evelyn", "Gabriel", "Grace",
    "Hannah", "Harper", "Henry", "Isabella", "Jack", "Jacob", "James",
    "Jason", "Jennifer", "Jessica", "John", "Jonathan", "Jordan", "Joseph",
    "Joshua", "Julia", "Justin", "Katherine", "Kevin", "Lauren", "Liam",
    "Lily", "Logan", "Lucas", "Madison", "Matthew", "Megan", "Michael",
    "Mia", "Natalie", "Nathan", "Nicholas", "Noah", "Olivia", "Owen",
    "Rachel", "Rebecca", "Ryan", "Samantha", "Samuel", "Sarah", "Sophia",
    "Taylor", "Thomas", "Victoria", "William", "Zoe",
)

LAST_NAMES = (
    "Adams", "Allen", "Anderson", "Baker", "Barnes", "Bell", "Bennett",
    "Brooks", "Brown", "Bryant", "Butler", "Campbell", "Carter", "Clark",
    "Collins", "Cook", "Cooper", "Cox", "Davis", "Diaz", "Edwards",
    "Evans", "Fisher", "Flores", "Foster", "Garcia", "Gonzalez", "Gray",
    "Green", "Griffin", "Hall", "Harris", "Hayes", "Henderson", "Hill",
    "Howard", "Hughes", "Jackson", "James", "Jenkins", "Johnson", "Jones",
    "Kelly", "King", "Lee", "Lewis", "Long", "Martin", "Martinez", "Miller",
    "Mitchell", "Moore", "Morgan", "Morris", "Murphy", "Nelson", "Parker",
    "Patterson", "Perry", "Peterson", "Phillips", "Powell", "Price",
    "Ramirez", "Reed", "Richardson", "Rivera", "Roberts", "Robinson",
    "Rodriguez", "Rogers", "Ross", "Russell", "Sanchez", "Sanders", "Scott",
    "Smith", "Stewart", "Taylor", "Thomas", "Thompson", "Turner", "Walker",
    "Ward", "Watson", "White", "Williams", "Wilson", "Wood", "Wright",
)

EMAIL_DOMAINS = (
    "example.com", "mail.example", "inbox.example", "post.example", "webmail.example"
)


@dataclass(slots=True)
class Profile:
    fio: str
    email: str
    login: str
    revision: int = 0


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("значение должно быть положительным целым числом")
    return parsed


def probability(value: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("вероятность должна быть в диапазоне от 0 до 1")
    return parsed


def parse_start_time(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "ожидается ISO 8601, например 2026-09-01T00:00:00Z"
        ) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("дата-время должна содержать часовой пояс")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Генерация CSV-лога событий для задания по SCD2. "
            "В каждом файле 1000 строк данных по умолчанию."
        )
    )
    parser.add_argument("--rows", required=True, type=positive_int, help="общее число строк")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("event_log"), help="каталог результата"
    )
    parser.add_argument(
        "--rows-per-file", type=positive_int, default=1000, help="строк данных в одном CSV"
    )
    parser.add_argument(
        "--users",
        type=positive_int,
        default=None,
        help="число возможных user_id; по умолчанию подбирается автоматически",
    )
    parser.add_argument(
        "--change-probability",
        type=probability,
        default=0.01,
        help="вероятность изменения одного атрибута при очередном событии пользователя",
    )
    parser.add_argument("--seed", type=int, default=42, help="seed для воспроизводимости")
    parser.add_argument(
        "--start-time",
        type=parse_start_time,
        default=parse_start_time("2026-09-01T00:00:00Z"),
        help="начало временной шкалы в ISO 8601 с часовым поясом",
    )
    parser.add_argument(
        "--max-step-seconds",
        type=positive_int,
        default=3,
        help="максимальный шаг времени между соседними событиями",
    )
    parser.add_argument(
        "--user-id-start", type=positive_int, default=1, help="минимальный user_id"
    )
    parser.add_argument(
        "--progress-every-files",
        type=positive_int,
        default=100,
        help="частота сообщений о прогрессе",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="заменить ранее созданные этим скриптом events_*.csv и _manifest.json",
    )
    return parser


def default_user_count(rows: int) -> int:
    return min(rows, max(100, min(100_000, rows // 20)))


def safe_token(value: str) -> str:
    return "".join(character.lower() for character in value if character.isascii() and character.isalnum())


def new_profile(rng: random.Random, user_id: int) -> Profile:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    stem = f"{safe_token(first)}.{safe_token(last)}"
    return Profile(
        fio=f"{first} {last}",
        email=f"{stem}.{user_id}@{rng.choice(EMAIL_DOMAINS)}",
        login=f"{safe_token(first[0])}{safe_token(last)}{user_id}",
    )


def mutate_profile(rng: random.Random, profile: Profile, user_id: int) -> None:
    profile.revision += 1
    field = rng.randrange(3)
    if field == 0:
        old_fio = profile.fio
        while profile.fio == old_fio:
            profile.fio = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    elif field == 1:
        first, last = profile.fio.split(" ", 1)
        stem = f"{safe_token(first)}.{safe_token(last)}"
        profile.email = (
            f"{stem}.{user_id}.r{profile.revision}@{rng.choice(EMAIL_DOMAINS)}"
        )
    else:
        first, last = profile.fio.split(" ", 1)
        profile.login = (
            f"{safe_token(first[0])}{safe_token(last)}{user_id}r{profile.revision}"
        )


def prepare_output(output_dir: Path, overwrite: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated = list(output_dir.glob("events_*.csv"))
    manifest = output_dir / "_manifest.json"
    partials = list(output_dir.glob("*.part"))
    if (generated or manifest.exists() or partials) and not overwrite:
        raise RuntimeError(
            f"В {output_dir} уже есть результат генерации. "
            "Выберите другой каталог или добавьте --overwrite."
        )
    if overwrite:
        for path in generated + partials:
            path.unlink()
        if manifest.exists():
            manifest.unlink()


def write_manifest(output_dir: Path, payload: dict[str, object]) -> None:
    target = output_dir / "_manifest.json"
    temporary = output_dir / "_manifest.json.part"
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    os.replace(temporary, target)


def generate(args: argparse.Namespace) -> None:
    user_count = args.users if args.users is not None else default_user_count(args.rows)
    if args.user_id_start + user_count - 1 > UINT32_MAX:
        raise ValueError("диапазон user_id выходит за пределы uint32")

    prepare_output(args.output_dir, args.overwrite)
    rng = random.Random(args.seed)
    profiles: list[Profile | None] = [None] * user_count
    file_count = math.ceil(args.rows / args.rows_per_file)
    filename_width = max(5, len(str(file_count)))
    current_time = args.start_time
    first_event_time: str | None = None
    last_event_time: str | None = None
    change_count = 0
    started_at = time.perf_counter()
    rows_written = 0

    for file_number in range(1, file_count + 1):
        remaining = args.rows - rows_written
        rows_in_file = min(args.rows_per_file, remaining)
        filename = f"events_{file_number:0{filename_width}d}.csv"
        target = args.output_dir / filename
        temporary = target.with_suffix(target.suffix + ".part")

        try:
            with temporary.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream, lineterminator="\n")
                writer.writerow(CSV_COLUMNS)
                for _ in range(rows_in_file):
                    current_time += timedelta(seconds=rng.randint(1, args.max_step_seconds))
                    offset = rng.randrange(user_count)
                    user_id = args.user_id_start + offset
                    profile = profiles[offset]
                    if profile is None:
                        profile = new_profile(rng, user_id)
                        profiles[offset] = profile
                    elif rng.random() < args.change_probability:
                        mutate_profile(rng, profile, user_id)
                        change_count += 1

                    event_time = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    if first_event_time is None:
                        first_event_time = event_time
                    last_event_time = event_time
                    writer.writerow(
                        (event_time, user_id, profile.fio, profile.email, profile.login)
                    )
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

        rows_written += rows_in_file
        if file_number % args.progress_every_files == 0 or file_number == file_count:
            elapsed = max(time.perf_counter() - started_at, 1e-9)
            print(
                f"files={file_number}/{file_count} rows={rows_written}/{args.rows} "
                f"rate={rows_written / elapsed:,.0f} rows/s",
                file=sys.stderr,
            )

    elapsed = time.perf_counter() - started_at
    write_manifest(
        args.output_dir,
        {
            "format": "csv",
            "columns": list(CSV_COLUMNS),
            "total_rows": args.rows,
            "rows_per_file": args.rows_per_file,
            "file_count": file_count,
            "user_count": user_count,
            "user_id_start": args.user_id_start,
            "change_probability": args.change_probability,
            "applied_profile_changes": change_count,
            "seed": args.seed,
            "first_event_time": first_event_time,
            "last_event_time": last_event_time,
            "elapsed_seconds": round(elapsed, 3),
        },
    )
    print(f"Готово: {args.rows} строк в каталоге {args.output_dir}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        generate(args)
    except (OSError, RuntimeError, ValueError) as exc:
        parser.exit(2, f"Ошибка: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
