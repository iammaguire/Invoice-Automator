import os
import sys
import PyPDF2
import platform
import subprocess
import pylightxl as xl
from typing import List
from pathlib import Path
from fuzzywuzzy import fuzz
from threading import Thread
import multiprocessing as mp
from functools import partial
from scipy.optimize import fmin
from dataclasses import dataclass
from PyQt5 import QtWidgets, QtGui, QtCore, uic

@dataclass
class Invoice:
    pageNum: int
    projectName: str
    projectNum: int
    invoiceNum: int

@dataclass
class TimeDetail:
    pages: List[int]
    projectNum: int

@dataclass
class PairedData:
    invoice: Invoice
    timeDetail: TimeDetail

def findPropertyNameEntryCandidates(db, pName, fuzzRatio):
    candidates = []
    for wsName in db.ws_names:
        for rowNum, row in enumerate(db.ws(wsName).col(col=3)[2:]):
            if fuzz.ratio(pName, row) >= fuzzRatio:
                candidates.append(db.ws(wsName).row(rowNum + 3))
    return candidates

    bestFullMatch = 0
    bestRatio = 0

def maxFullAndPartial(fuzzRatio, db, pairedData):
    partialMatched = 0
    fullMatched = 0
    noMatch = 0
    fMatch = []
    partialMatchCounts = []
    pairCandidates = []
    for pair in pairedData:
        pName = pair.invoice.projectName
        candidates = findPropertyNameEntryCandidates(db, pName, fuzzRatio)
        if len(candidates) == 0:
            for word in pName.split(' '):
                if len(word) > 5:
                    candidates += findPropertyNameEntryCandidates(db, word, fuzzRatio)
        if len(candidates) > 1:
            partialMatched += 1
            partialMatchCounts.append(len(candidates))
        elif len(candidates) == 1:
            fullMatched += 1
            fMatch.append((pair.invoice, candidates))
        elif len(candidates) == 0:
            noMatch += 1
        pairCandidates.append((pair, candidates))
        
    try:
        fullMatchRatio = fullMatched / len(pairedData)
        partialMatchRatio = partialMatched / len(pairedData)
        partialMatchAvg = sum(partialMatchCounts) / len(partialMatchCounts)
        retStr = 'Partial match for ' + str(int(partialMatchRatio * 100)) + '% with avg. count ' + str(int(partialMatchAvg)) + '. Min: ' + str(min(partialMatchCounts)) + ', Max: ' + str(max(partialMatchCounts)) + '\nFull match for ' + str(int(fullMatchRatio * 100)) + '%' + '\nNo match for ' + str(int(noMatch / len(pairedData) * 100)) + '%'
        
        return (fuzzRatio, fullMatched, pairCandidates, retStr)
    except:
        return (0, 0, None, '')

class UI(QtWidgets.QMainWindow):
    def __init__(self):
        super(UI, self).__init__()
        uic.loadUi('uilayout.ui', self)

        self.statusBar.showMessage('')
        self.setWindowIcon(QtGui.QIcon('horse.jpg'))
        self.invoiceButton.clicked.connect(self.browseInvoices)
        self.timeDetailButton.clicked.connect(self.browseTimeDetail)
        self.assetAssignmentsButton.clicked.connect(self.browseAssetAssignments)
        self.outputButton.clicked.connect(self.browseOutputFolder)
        self.executeButton.clicked.connect(self.execute)
        self.openOutputButton.clicked.connect(self.openOutput)
        self.progressBar.setValue(0)
        self.openOutputButton.setEnabled(False)
        self.analysisGranularity = 5

        self.invoicePath = 'C:/Users/Connor/Documents/Programming/Python/PGIM/invoices.pdf'
        self.timeDetailPath = 'C:/Users/Connor/Documents/Programming/Python/PGIM/time_detail.pdf'
        self.assetAssignmentsPath = 'C:/Users/Connor/Documents/Programming/Python/PGIM/asset_assignments.xlsx'
        self.outputFolderPath = 'C:/Users/Connor/Documents/Programming/Python/PGIM/output'

        self.invoiceText.setText('C:/Users/Connor/Documents/Programming/Python/PGIM/invoices.pdf')
        self.timeDetailText.setText('C:/Users/Connor/Documents/Programming/Python/PGIM/time_detail.pdf')
        self.assetAssignmentText.setText('C:/Users/Connor/Documents/Programming/Python/PGIM/asset_assignments.xlsx')
        self.outputText.setText('C:/Users/Connor/Documents/Programming/Python/PGIM/output')

        self.show()

    def execute(self):
        if not Path(self.invoiceText.text()).is_file() or self.invoiceText.text().split('.')[1] != 'pdf':
            QtWidgets.QMessageBox().critical(self, 'Error', 'Invoice file ' + self.invoiceText.text() + ' does not exist or isn\'t a valid PDF file.')
            return
        
        if not Path(self.timeDetailText.text()).is_file() or self.timeDetailText.text().split('.')[1] != 'pdf':
            QtWidgets.QMessageBox().critical(self, 'Error', 'Time detail file ' + self.timeDetailText.text() + ' does not exist or isn\'t a valid PDF file.')
            return

        if not Path(self.assetAssignmentText.text()).is_file() or self.assetAssignmentText.text().split('.')[1] != 'xlsx':
            QtWidgets.QMessageBox().critical(self, 'Error', 'Asset assignments file ' + self.assetAssignmentText.text() + ' does not exist or isn\'t a valid XLSX file.')
            return

        if not Path(self.outputText.text()).is_dir() or self.outputText.text() == '':
            QtWidgets.QMessageBox().critical(self, 'Error', 'Output directory ' + self.outputText.text() + ' does not exist')
            return
        
        self.invoices = self.extractInvoices()
        self.timeDetail = self.extractTimeDetail()
        self.pairedData = self.pairData()
        self.mergePairs()

        self.statusBar.showMessage('Running statistical analysis...')
        self.db = xl.readxl(fn=self.assetAssignmentText.text())
        
        pairCandidates = list()
        self.statisticalAnalysis(self.db, self.pairedData, pairCandidates)
        pairCandidatesList = pairCandidates[:-1]
        QtWidgets.QMessageBox().information(self, 'Analysis details', pairCandidates[len(pairCandidates) - 1])

        self.openOutputButton.setEnabled(True)
        self.statusBar.showMessage('Done. I love you Mom!')

    def openOutput(self):
        if platform.system() == 'Windows':
            os.startfile(self.outputText.text())
        else:
            subprocess.call(['open', '-R', self.outputText.text()])

    def browseInvoices(self):
        self.invoicePath, _ = QtWidgets.QFileDialog.getOpenFileName(self, 'Single File', QtCore.QDir.rootPath() , '*.pdf')
        self.invoiceText.setText(self.invoicePath)

    def browseTimeDetail(self):
        self.timeDetailPath, _ = QtWidgets.QFileDialog.getOpenFileName(self, 'Single File', QtCore.QDir.rootPath() , '*.pdf')       
        self.timeDetailText.setText(self.timeDetailPath)       
    
    def browseAssetAssignments(self):
        self.assetAssignmentsPath, _ = QtWidgets.QFileDialog.getOpenFileName(self, 'Single File', QtCore.QDir.rootPath() , '*.xlsx')       
        self.assetAssignmentText.setText(self.assetAssignmentsPath)       

    def browseOutputFolder(self):
        self.outputFolderPath = QtWidgets.QFileDialog.getExistingDirectory(self, 'Select Folder')
        self.outputText.setText(self.outputFolderPath)

    def extractInvoices(self):
        parsedInvoices = []
        hInvoices = open(self.invoicePath, 'rb')
        invoicesReader = PyPDF2.PdfFileReader(hInvoices)
        self.statusBar.showMessage('Parsing invoices from ' + self.invoicePath + '...')
        
        for pageNum in range(invoicesReader.numPages):
            self.progressBar.setValue(int(pageNum / invoicesReader.numPages * 100))
            invoicesZ = invoicesReader.getPage(pageNum)
            lines = invoicesZ.extractText().split('\n')
            invoiceNum = lines[lines.index('Invoice No.') + 1]
            pNameNum = lines[lines.index('Project No.') + 1]
            pName = ' '.join(pNameNum.split(' ')[1:])
            pNum = pNameNum.split(' ')[0]
            parsedInvoices.append(Invoice(pageNum, pName, pNum, invoiceNum))

        self.progressBar.setValue(100)
        hInvoices.close()
        return parsedInvoices

    def extractTimeDetail(self):
        hTimeDetail = open(self.timeDetailPath, 'rb')
        timeDetailReader = PyPDF2.PdfFileReader(hTimeDetail)
        timeDetails = []
        self.statusBar.showMessage('Parsing time detail from ' + self.timeDetailPath + '...')

        for pageNum in range(timeDetailReader.numPages):
            self.progressBar.setValue(int(pageNum / timeDetailReader.numPages * 100))
            td0 = timeDetailReader.getPage(pageNum)
            lines = td0.extractText().split('\n')
            
            notesIdx = lines.index('Notes')
            onNewPage = lines[notesIdx + 1].split(' ')[0] == 'PGIM'
            
            if onNewPage:
                pNum = lines[notesIdx + 1]
                nameDetails = pNum.split(':')[1].split(' ')
                if nameDetails[1] == 'PK':
                    pNamePrefix = lines[notesIdx+1].split(':')[2]
                    pNameSuffix = lines[notesIdx+2]
                    middleChar = pNamePrefix[len(pNamePrefix) - 1]
                    if str.isalpha(middleChar):
                        pName = pNamePrefix[:-2]
                    elif middleChar == ' ':
                        pName = pNamePrefix.strip()
                    else: # pNamePrefix last char is '.' or numeric
                        specialIdx = 0
                        pName = pNamePrefix
                        while pNameSuffix[specialIdx] == '.' or str.isnumeric(pNameSuffix[specialIdx]):
                            pName += pNameSuffix[specialIdx]
                            specialIdx += 1
                else:
                    pName = nameDetails[0]
                timeDetail = TimeDetail([pageNum], pName)
                timeDetails.append(timeDetail)
            else:
                timeDetails[len(timeDetails) - 1].pages.append(pageNum)
 
        self.progressBar.setValue(100)
        hTimeDetail.close()
        return timeDetails

    def pairData(self):
        pairedData = []
        self.statusBar.showMessage('Pairing invoices and time details...')

        for i, inv in enumerate(self.invoices):
            self.progressBar.setValue(int(i / len(self.invoices) * 100))
            pairedTimeDetail = None
            for td in self.timeDetail:
                if inv.projectNum == td.projectNum:
                    pairedTimeDetail = td
            if pairedTimeDetail == None:
                print('Couldn\'t find matching time detail for invoice!')
                print('Page ' + str(inv.pageNum + 1) + ': Invoice ' + inv.invoiceNum + ' for ' + inv.projectNum + ' ' + inv.projectName)
            else:
                pairedData.append(PairedData(inv, pairedTimeDetail))

        self.progressBar.setValue(100)
        return pairedData

    def mergePairs(self):
        invoiceReader = PyPDF2.PdfFileReader(self.invoicePath)
        timeDetailReader = PyPDF2.PdfFileReader(self.timeDetailPath)
        
        self.statusBar.showMessage('Merging pairs to disk...')
        for i, pair in enumerate(self.pairedData):
            self.progressBar.setValue(int(i / len(self.pairedData) * 100))
            invoicePage = pair.invoice.pageNum
            timeDetailPages = pair.timeDetail.pages
            pdfWriter = PyPDF2.PdfFileWriter()
            pdfWriter.addPage(invoiceReader.getPage(invoicePage))
        
            for p in timeDetailPages:
                pdfWriter.addPage(timeDetailReader.getPage(p))
            with Path(self.outputFolderPath + '/ASM_AN_' + pair.invoice.projectNum + '_' + pair.invoice.projectName.replace('\\', '.').replace('/', '.') + '_' + str(pair.invoice.invoiceNum) + '.pdf').open('wb') as outputFile:
                pdfWriter.write(outputFile)
        self.progressBar.setValue(100)

    def statisticalAnalysis(self, dbD, pairedDataD, outputList):
        print('Running statistical analysis...')
        pool = mp.Pool(processes=16)
        results = pool.map(partial(maxFullAndPartial, db=dbD, pairedData=pairedDataD), range(30, 72, self.analysisGranularity))
        bestPair = (0, 0)
        
        for r in results:
            if bestPair[1] < r[1]:
                bestPair = r

        print('Best ratio: ' + str(bestPair[0]))
        final = maxFullAndPartial(bestPair[0], db=dbD, pairedData=pairedDataD)
        outputList.append(final[2])
        outputList.append(final[3])

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = UI()
    app.exec_()
