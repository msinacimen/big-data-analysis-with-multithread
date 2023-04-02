from math import floor
from multiprocessing import Process, cpu_count
from time import sleep, perf_counter
from similarity import similar
import numpy as np
import pandas as pd


similars = []



class ThreadController(Process):
    def __init__(self, name, part, distinctproducts):
        Process.__init__(self)
        self.name = name
        self.part = part
        self.distinctproducts = distinctproducts
        self.products = self.part['Product'].tolist()
        self.threadstart = perf_counter()
        self.similars = []
        self.threadend = 0
        self.running = True
        # print(distinctproducts)

    def run(self):
        while self.running:
            j = 0
            while j < len(self.distinctproducts):
                record2 = self.distinctproducts[j]
                for k in range(len(self.products)):
                    record1 = self.products[k]
                    # if record1 == record2:
                    #     pass
                    if similar(record1, record2) > 60.0:
                        self.similars.append([record1, record2, similar(record1, record2)])
                # print(f'{self.name} is running in {j}th iteration')
                j += 1
            # print(len(self.similars), self.name)
            self.threadend = perf_counter()
            # sleep(1)
            self.running = False

    def stop(self):
        self.running = False


if __name__ == '__main__':
    # with Manager() as manager:
    #     similars = manager.list()
    numofthreads = min(int(input("How many threads do you want to run?process ")), floor(2 * cpu_count() / 3))
    df = pd.read_csv("clrdata.csv")
    partition = np.array_split(df, numofthreads)
    print("Every thread gets ", len(partition[0]), " records")
    threads = []
    distinctdf = pd.read_csv("distinct.csv",
                             dtype={'Product': str, 'Issue': str, 'Company': str, 'State': str, 'Complaint ID': int,
                                    'ZIP code': str})
    distinctproducts = distinctdf['Product']
    distinctproducts = distinctproducts.dropna(axis=0).tolist()
    print("Distinct products: ", len(distinctproducts))
    starttimer = perf_counter()
    for i in range(numofthreads):
        threads.append(ThreadController("Thread " + str(i), partition[i], distinctproducts))
        threads[i].start()
        print("Thread " + str(i) + " started with id:" + str(threads[i].ident))
    for i in range(numofthreads):
        threads[i].join()
        threads[i].threadend = perf_counter()
        if threads[i].threadend > 0:
            print(f"Thread {i} finished in {threads[i].threadend - threads[i].threadstart:0.4f} seconds")
    endtimer = perf_counter()
    print("All threads have finished")
    print(f"Time taken: {(endtimer - starttimer):0.4f} seconds")
    sleep(1)
    for i in range(numofthreads):
        threads[i].stop()
        print("Thread " + str(i) + " stopped")
    # similarsdf = pd.DataFrame(similars, columns=['Product1', 'Product2', 'Similarity'])
    # similarsdf.drop_duplicates()
    print(len(similars))
    # print(*similars, sep='\n')