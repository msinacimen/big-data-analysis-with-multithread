from threading import Thread
from time import sleep, perf_counter, process_time

import numpy as np
import pandas as pd

from similarity import similar

similars = []


class ThreadController(Thread):
    def __init__(self, name, part, dataframe):
        Thread.__init__(self)
        self.name = name
        self.part = part
        self.dataframe = dataframe
        self.products = self.part['Product'].tolist()
        self.allproducts = self.dataframe['Product'].tolist()
        self.threadstart = process_time()
        self.running = True
        # print(distinctproducts)

    def run(self):
        while self.running:
            j = 0
            while j < len(distinctproducts):
                record2 = distinctproducts[j]
                for k in range(len(self.products)):
                    record1 = self.products[k]
                    # if record1 == record2:
                    #     pass
                    if similar(record1, record2) > 60.0:
                        similars.append([record1, record2, similar(record1, record2)])
                # print(f'{self.name} is running in {j}th iteration')
                j += 1
            self.threadend = process_time()
            # sleep(1)
            self.running = False

    def stop(self):
        self.running = False


if __name__ == '__main__':
    numofthreads = int(input("How many threads do you want to run? "))
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
        threads.append(ThreadController("Thread " + str(i), partition[i], df))
        threads[i].start()
        print("Thread " + str(i) + " started with id:" + str(threads[i].ident))
    for i in range(numofthreads):
        threads[i].join()
        print(f"Thread {i} has finished it took   {(threads[i].threadend - threads[i].threadstart):0.4f}   seconds")
    endtimer = perf_counter()
    print("All threads have finished")
    print(f"Time taken: {(endtimer - starttimer):0.4f} seconds")
    sleep(1)
    for i in range(numofthreads):
        threads[i].stop()
        print("Thread " + str(i) + " stopped")
    print(len(similars))
    # print(*similars, sep='\n')
