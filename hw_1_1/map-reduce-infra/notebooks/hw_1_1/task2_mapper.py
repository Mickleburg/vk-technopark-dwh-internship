
import sys
import csv


def mapper():
    reader = csv.reader(sys.stdin, delimiter=';')
    for row in reader:
        link, date = row
        date = date.split()[0]
        print(f"{date}\x1f{link}\t1")


if __name__ == "__main__":
    mapper()
