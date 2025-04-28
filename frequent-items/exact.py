#!/usr/bin/env python3
import sys 

def exact(thres: float, datapath: str) -> list[tuple[int, float]]:
    # iterate over dataset, counting item occurrences
    counts = {}
    transactions = 0
    with open(datapath, 'r') as data:
        for transaction in data:
            transactions += 1
            for item in transaction.split(" ")[:-1]:
                counts[item] = counts.get(item, 0) + 1

    # add items and frequencies to output
    frequent_items = []
    for item, count in counts.items():
        if count / transactions >= thres:
            frequent_items.append((int(item), float(count / transactions)))
    frequent_items.sort(key=lambda x: (x[1], -x[0]), reverse=True)
    return frequent_items

def main():
    # pass args to function if called as script
    if len(sys.argv) != 3:
        sys.exit("check arguments") 
    thres = float(sys.argv[1])
    datapath = sys.argv[2]
    print(exact(thres, datapath))

if __name__ == "__main__":
    main()
