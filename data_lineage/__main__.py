import sys

from data_lineage.data_lineage import fib

if __name__ == "__main__":
    n = int(sys.argv[1])
    print(fib(n))
