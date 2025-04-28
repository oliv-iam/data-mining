#!/usr/bin/env python3
import sys
import random
import math

def sampling(ssize: int, dsize: int, delta: float, thres: float, datapath: str) -> tuple[float, list[tuple[int, float]]]:
    # randomly choose transactions to include
    sidcs = {}
    for i in range(ssize):
        rand = random.randint(0, dsize - 1)
        sidcs[rand] = sidcs.get(rand, 0) + 1

    # obtain sample counts, max|t|
    didx = 0
    scounts = {}
    maxtrans = 0
    with open(datapath, 'r') as data:
        for transaction in data:
            if sidcs.get(didx, 0) > 0:
                t = transaction.split(" ")[:-1]
                maxtrans = max(maxtrans, len(t))
                for item in t:
                    scounts[item] = scounts.get(item, 0) + sidcs[didx]
            didx += 1
    
    # calculate eps, sample thres
    ds = int(math.log2(maxtrans) + 1)
    eps = math.sqrt((2/ssize) * (ds + math.log(1/delta)))
    sthres = thres - eps / 2

    # exact frequent items from sample, output [eps, frequent]
    frequent_items = []
    for item, count in scounts.items():
        if count / ssize >= sthres:
            frequent_items.append((int(item), float(count / ssize)))
    frequent_items.sort(key=lambda x: (x[1], -x[0]), reverse=True)
    return [eps, frequent_items]

def main():
    # pass arguments to function if called as script
    if len(sys.argv) != 6:
        sys.exit("check arguments")
    ssize = int(sys.argv[1])
    dsize = int(sys.argv[2])
    delta = float(sys.argv[3])
    thres = float(sys.argv[4])
    datapath = str(sys.argv[5])
    print(sampling(ssize, dsize, delta, thres, datapath))

if __name__ == "__main__":
    main()
