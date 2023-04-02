import os
from math import floor
from multiprocessing import Process, cpu_count, Pipe
from time import perf_counter
from threading import Thread
import qdarkstyle
from PyQt5.QtWidgets import *
from similarity import similar
import numpy as np
import pandas as pd

similars = []


def worker(part, distincts, conn, selection, threshold, appendstyle, preprocesslist, checklist):
    class threadwindow(QMainWindow):
        def __init__(self):
            super().__init__()
            self.setWindowTitle("Thread Window")
            self.resize(400, 200)
            self.part = part
            self.distincts = distincts
            self.name = QLabel(self)
            self.name.setText("Thread is running with id: " + str(os.getpid()) + "     ")
            self.name.move(50, 50)
            self.name.adjustSize()
            self.comparsionlabel = QLabel(self)
            self.comparsionlabel.setText("Starting comparisons with " + str(len(self.part)) + " records    ")
            self.comparsionlabel.move(50, 100)
            self.comparsionlabel.adjustSize()
            self.initUI()

        def threads(self):
            self.thread = Thread(target=self.dothings)
            self.thread.start()

        def initUI(self):
            self.progressbar = QProgressBar(self)
            self.progressbar.setGeometry(50, 150, 300, 30)
            self.progressbarchangeval(0)
            self.threads()

        def progressbarchangeval(self, value):
            self.progressbar.setValue(value)

        def dothings(self):
            print("Thread is running with id: " + str(os.getpid()))
            selectlist = ['Product', 'Issue', 'Company', 'State', 'Complaint ID', 'ZIP code']
            if selection > 6 or selection < 0:
                print("Invalid selection")
                return

            def append(appendstyle, record1, record2, k):
                appendstylelist = ['Product', 'Issue', 'Company', 'State', 'Complaint ID', 'ZIP code', 'All']
                if appendstyle < 6 and appendstyle >= 0:
                    similars.append(
                        [self.partnew.iloc[k][appendstyle]] + [record2, similar(record1, record2)])
                elif appendstyle == 6:
                    toappend = self.partnew.iloc[k].tolist()
                    toappend.append(record2)
                    toappend.append(similar(record1, record2))
                    similars.append(toappend)
                else:
                    print("Invalid appendstyle")
                    return

            def comparisons(distinctcomparison, compare):
                global window
                for j in range(len(distinctcomparison)):
                    record2 = distinctcomparison[j]
                    for k in range(len(compare)):
                        record1 = compare[k]
                        if similar(record1, record2) >= threshold:
                            append(appendstyle, record1, record2, k)
                    # show progress in ui
                    progress = (j + 1) / len(distinctcomparison) * 100
                    progress = floor(progress)
                    self.progressbarchangeval(progress)
                    # print(f'{j}th iteration. completed percentage: {j / len(distinctcomparison) * 100}, for process {os.getpid()}')

            def preprocess(select, name, distincts, part):
                newlistp = []
                newlistd = []
                for i in range(len(part)):
                    if name.lower() in part.iloc[i][selectlist[select]].lower():
                        newlistp.append(part.iloc[i].tolist())
                part = pd.DataFrame(newlistp, columns=part.columns)
                for i in range(len(distincts)):
                    if pd.isna(distincts[selectlist[select]][i]):
                        print("NaN")
                        break
                    if name.lower() in distincts.iloc[i][selectlist[select]].lower():
                        newlistd.append(distincts.iloc[i].tolist())
                distincts = pd.DataFrame(newlistd, columns=distincts.columns)
                print("Preprocessing done for ", name)
                return part, distincts

            def precomparison(distinctcomparison, compare, index):
                print("inside precomparison")
                same = []
                for j in range(len(distinctcomparison)):
                    record2 = distinctcomparison[j]
                    for k in range(len(compare)):
                        record1 = compare[k]
                        if record1 == record2:
                            row = self.partnew.iloc[k].tolist()
                            row[index] = record1
                            same.append(row)
                partsame = pd.DataFrame(same)
                return partsame

            index = 0
            self.partnew = self.part
            self.distinctsnew = self.distincts
            for i in preprocesslist:
                if i != "":
                    self.partnew, self.distinctsnew = preprocess(index, i, self.distincts, self.part)
                index += 1

            index = 0
            for j in checklist:
                if j is True:
                    print("Precomparison started for ", selectlist[index])
                    precompare = self.partnew.iloc[:, index].tolist()
                    predistinctcomparison = self.distinctsnew.iloc[:, index]
                    predistinctcomparison = predistinctcomparison.dropna(axis=0).tolist()
                    self.partnewegg = precomparison(predistinctcomparison, precompare, index)
                    print("test")
                index += 1

            compare = self.partnew.iloc[:, selection].tolist()
            distinctcomparison = self.distinctsnew.iloc[:, selection]
            distinctcomparison = distinctcomparison.dropna(axis=0).tolist()
            print("Starting comparisons with ", len(compare), " records")
            comparisons(distinctcomparison, compare)
            print(len(similars), f" records found for thread {conn}")
            self.close()
            conn.send(similars)
            conn.close()

    app = QApplication([])
    window = threadwindow()
    window.show()
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())
    app.exec()

    # for i in preprocesslist:
    #     index = 0
    #     if i != "":
    #         part, distincts = preprocess(index, i, distincts, part)
    #     index += 1
    # compare = part[selectlist[selection]].tolist()
    # distinctcomparison = distincts[selectlist[selection]]
    # distinctcomparison = distinctcomparison.dropna(axis=0).tolist()
    # print("Starting comparisons with ", len(compare), " records")
    # comparisons()
    # print(len(similars), f" records found for thread {conn}")
    # conn.send(similars)
    # conn.close()


def startthreads(numofthreads, threshold, selection, output, preprocess, checklist):
    # numofthreads = min(int(input("How many threads do you want to run?test ")), floor(2 * cpu_count() / 3))
    numofthreads = min(numofthreads, floor(2 * cpu_count() / 3))
    df = pd.read_csv("clrdata.csv")
    partition = np.array_split(df, numofthreads)
    print("Every thread gets ", len(partition[0]), " records")
    threads = []
    pipe_list = []
    distinctdf = pd.read_csv("distinct.csv",
                             dtype={'Product': str, 'Issue': str, 'Company': str, 'State': str, 'Complaint ID': int,
                                    'ZIP code': str})
    starttime = perf_counter()
    for i in range(numofthreads):
        parent_conn, child_conn = Pipe(duplex=False)
        thread = Process(target=worker,
                         args=(
                         partition[i], distinctdf, child_conn, selection, threshold, output, preprocess, checklist))
        threads.append(thread)
        pipe_list.append(parent_conn)
        thread.start()
        thread.starttime = perf_counter()
        print(f'{thread.name} is started with {thread.pid} PID')
    results = [pipe.recv() for pipe in pipe_list]
    for thread in threads:
        thread.join()
        thread.endtime = perf_counter()
        print(f'{thread.name} is finished in {thread.endtime - thread.starttime:0.4f} seconds')
    endtime = perf_counter()
    senddata = []
    for i in results:
        for j in i:
            senddata.append(j)
    print(sum(len(row) for row in results), f" similar records found in {endtime - starttime:0.4f} seconds")
    return senddata
    # for i in results:
    #     for j in i:
    #         print(j)


if __name__ == '__main__':
    startthreads(10, 60, 1, 1, ["", "free", "", "", "", ""], [1, 0, 0, 0, 0, 0])
