
import sys


def reducer():
    count = 0

    for line in sys.stdin:
        _word, cnt = line.strip().split('\t', 1)
        count += int(cnt)

    print(f"Count: {count}")


if __name__ == "__main__":
    reducer()
