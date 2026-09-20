
import sys
import heapq


def reducer():
    current_date = ""
    current_site = ""
    current_cnt = 0
    top5 = []

    for line in sys.stdin:
        text, cnt = line.strip().split('\t', 1)
        date, site = text.split('\x1f')

        if date != current_date:
            print(current_date)
            for count, link in sorted(top5, key=lambda x: -x[0]):
                print(link, "---", count)
            print("===")
            current_date = date
            top5 = []
        elif site != current_site:
            item = (current_cnt, current_site)
            if len(top5) < 5:
                heapq.heappush(top5, item)
            elif current_cnt > top5[0][0]:
                heapq.heapreplace(top5, item)

            current_site = site
            current_cnt = int(cnt)
        else:
            current_cnt += int(cnt)


if __name__ == "__main__":
    reducer()            
