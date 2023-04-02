import multiprocessing
import time
import qdarkstyle
from PyQt5.QtWidgets import *
from PyQt5 import QtGui
from multiprocess import startthreads


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Multithreaded Application")
        self.setWindowIcon(QtGui.QIcon('Kouyenilogo.png'))
        self.setGeometry(100, 100, 400, 500)

        # Number of Thread
        self.numofthreadlabel = QLabel(self)
        self.numofthreadlabel.setText('Number\nof Thread:')
        self.numofthreadbox = QLineEdit(self)
        self.numofthreadbox.setPlaceholderText('Number of Thread')
        self.numofthreadbox.setValidator(QtGui.QIntValidator())

        self.numofthreadbox.move(100, 20)
        self.numofthreadbox.resize(200, 30)
        self.numofthreadlabel.move(20, 20)

        # Similarity
        self.similaritylabel = QLabel(self)
        self.similaritylabel.setText('Similarity:')
        self.similaritybox = QLineEdit(self)
        self.similaritybox.setText('50.0')

        self.similaritybox.move(100, 60)
        self.similaritybox.resize(200, 30)
        self.similaritylabel.move(20, 60)

        # Input
        self.inputlabel = QLabel(self)
        self.inputlabel.setText('Input')
        self.inputbox = QComboBox(self)
        self.inputbox.addItems(('Product', 'Issue', 'Company', 'State', 'Zip Code', 'Complaint ID'))

        self.inputbox.move(100, 100)
        self.inputbox.resize(200, 30)
        self.inputlabel.move(20, 100)

        # Output
        self.outputlabel = QLabel(self)
        self.outputlabel.setText('Output')
        self.outputbox = QComboBox(self)
        self.outputbox.addItems(('Product', 'Issue', 'Company', 'State', 'Zip Code', 'Complaint ID', 'All'))

        self.outputbox.move(100, 140)
        self.outputbox.resize(200, 30)
        self.outputlabel.move(20, 140)

        # Product
        self.productlabel = QLabel(self)
        self.productlabel.setText('Product:')
        self.productbox = QLineEdit(self)
        self.productcheckbox = QCheckBox(self)

        self.productbox.move(100, 180)
        self.productbox.resize(200, 30)
        self.productlabel.move(20, 180)
        self.productcheckbox.move(310, 180)

        # Issue
        self.issuelabel = QLabel(self)
        self.issuelabel.setText('Issue:')
        self.issuebox = QLineEdit(self)
        self.issuecheckbox = QCheckBox(self)

        self.issuebox.move(100, 220)
        self.issuebox.resize(200, 30)
        self.issuelabel.move(20, 220)
        self.issuecheckbox.move(310, 220)

        # Company
        self.companylabel = QLabel(self)
        self.companylabel.setText('Company:')
        self.companybox = QLineEdit(self)
        self.companycheckbox = QCheckBox(self)

        self.companybox.move(100, 260)
        self.companybox.resize(200, 30)
        self.companylabel.move(20, 260)
        self.companycheckbox.move(310, 260)

        # State
        self.statelabel = QLabel(self)
        self.statelabel.setText('State:')
        self.statebox = QLineEdit(self)
        self.statecheckbox = QCheckBox(self)

        self.statebox.move(100, 300)
        self.statebox.resize(200, 30)
        self.statelabel.move(20, 300)
        self.statecheckbox.move(310, 300)

        # Zip Code
        self.zipcodelabel = QLabel(self)
        self.zipcodelabel.setText('Zip Code:')
        self.zipcodebox = QLineEdit(self)
        self.zipcodecheckbox = QCheckBox(self)

        self.zipcodebox.move(100, 340)
        self.zipcodebox.resize(200, 30)
        self.zipcodelabel.move(20, 340)
        self.zipcodecheckbox.move(310, 340)

        # Complaint ID
        self.complaintidlabel = QLabel(self)
        self.complaintidlabel.setText('Complaint ID:')
        self.complaintidbox = QLineEdit(self)
        self.complaintidcheckbox = QCheckBox(self)

        self.complaintidbox.move(100, 380)
        self.complaintidbox.resize(200, 30)
        self.complaintidlabel.move(20, 380)
        self.complaintidcheckbox.move(310, 380)

        # Run
        self.runbutton = QPushButton('Run', self)
        self.runbutton.move(100, 420)
        self.runbutton.resize(200, 30)
        self.runbutton.clicked.connect(self.senddata)

    def senddata(self):
        print('send data')
        self.numofthread = int(self.numofthreadbox.text())
        self.similarity = float(self.similaritybox.text())
        self.input = self.inputbox.currentIndex()
        self.output = self.outputbox.currentIndex()
        self.product = self.productbox.text()
        self.issue = self.issuebox.text()
        self.company = self.companybox.text()
        self.state = self.statebox.text()
        self.zipcode = self.zipcodebox.text()
        self.complaintid = self.complaintidbox.text()
        self.productcheck = self.productcheckbox.isChecked()
        self.issuecheck = self.issuecheckbox.isChecked()
        self.companycheck = self.companycheckbox.isChecked()
        self.statecheck = self.statecheckbox.isChecked()
        self.zipcodecheck = self.zipcodecheckbox.isChecked()
        self.complaintidcheck = self.complaintidcheckbox.isChecked()
        print(self.numofthread)
        print(self.similarity)
        print(self.input)
        print(self.output)
        print(self.product)
        print(self.issue)
        print(self.company)
        print(self.state)
        print(self.zipcode)
        print(self.complaintid)
        print(self.productcheck)
        checklist = [self.productcheck, self.issuecheck, self.companycheck, self.statecheck, self.zipcodecheck, self.complaintidcheck]
        preprocess = [self.product, self.issue, self.company, self.state, self.zipcode, self.complaintid]
        starttime = time.perf_counter()
        data = startthreads(self.numofthread, self.similarity, self.input, self.output, preprocess, checklist)
        endtime = time.perf_counter()
        timetook = endtime - starttime
        self.showdata(data, timetook)

    def showdata(self, data, timetook):
        print('show data')
        self.sub_window = datawindow(data, timetook, self.outputbox.currentIndex() , self.outputbox.currentText())
        self.sub_window.show()


print(multiprocessing.current_process().name)

class datawindow(QMainWindow):
    def __init__(self, data, timetook, output, outputname):
        super().__init__()
        self.title = 'Data'
        self.data = data
        self.timetook = timetook
        self.output = output
        self.outputname = outputname
        self.initUI()
        self.showdata()

    def initUI(self):
        self.setWindowTitle(self.title)
        self.setGeometry(150, 150, 550, 600)

    def showdata(self):
        self.table = QTableWidget(self)
        self.table.setRowCount(len(self.data))
        self.table.setColumnCount(len(self.data[0]))
        self.table.move(20, 20)
        self.table.resize(500, 550)
        self.timelabel = QLabel(self)
        self.timelabel.setText('Time took: ' + str(self.timetook))
        self.timelabel.move(20, 570)
        if self.output < 6 and self.output >= 0:
            self.table.setHorizontalHeaderLabels([self.outputname, 'Similar with', 'Similarity'])
        else:
            self.resize(1050, 600)
            self.table.resize(1000, 550)
            self.table.setHorizontalHeaderLabels(['Product', 'Issue', 'Company', 'State', 'Zip Code',
                                                  'Complaint ID', 'Similar with', 'Similarity'])
        for i in range(len(self.data)):
            for j in range(len(self.data[0])):
                self.table.setItem(i, j, QTableWidgetItem(str(self.data[i][j])))
        self.table.show()


def startapp():
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())
    app.exec()
    print('Your name: ' + window.numofthreadbox.text())
    print(window.inputbox.currentIndex())


if multiprocessing.current_process().name == 'MainProcess':
    startapp()
