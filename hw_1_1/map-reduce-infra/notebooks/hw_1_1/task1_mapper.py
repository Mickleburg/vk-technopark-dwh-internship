
import sys


def mapper():
    for line in sys.stdin:
        for word in line.strip().split():
            if len(word) > 4:
                print(f"{word}\t1")


if __name__ == "__main__":
    mapper()
