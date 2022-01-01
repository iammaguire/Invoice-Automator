import os
import PyPDF2
import argparse
from tqdm import tqdm
import pylightxl as xl
from typing import List
from pathlib import Path
from fuzzywuzzy import fuzz
import multiprocessing as mp
from functools import partial
from scipy.optimize import fmin
from dataclasses import dataclass

@dataclass
class Invoice:
    pageNum: int
    projectName: str
    projectNum: int
    invoiceNum: int
    amEmail: str

@dataclass
class TimeDetail:
    pages: List[int]
    projectNum: int

@dataclass
class PairedData:
    invoice: Invoice
    timeDetail: TimeDetail

def extractInvoices(invoiceFname):
    parsedInvoices = []
    hInvoices = open(invoiceFname, 'rb')
    invoicesReader = PyPDF2.PdfFileReader(hInvoices)
    print('Parsing invoices from ' + invoiceFname + '...')
    
    for pageNum in tqdm(range(invoicesReader.numPages)):
        invoicesZ = invoicesReader.getPage(pageNum)
        lines = invoicesZ.extractText().split('\n')
        invoiceNum = lines[lines.index('Invoice No.') + 1]
        pNameNum = lines[lines.index('Project No.') + 1]
        try:
            amEmail = lines[lines.index('PGIM Real Estate') + 1]
        except:
            amEmail = ''
        pName = ' '.join(pNameNum.split(' ')[1:])
        pNum = pNameNum.split(' ')[0]
        parsedInvoices.append(Invoice(pageNum, pName, pNum, invoiceNum, amEmail))
    
    hInvoices.close()
    return parsedInvoices

def extractTimeDetail(timeDetailFname):
    hTimeDetail = open(timeDetailFname, 'rb')
    timeDetailReader = PyPDF2.PdfFileReader(hTimeDetail)
    timeDetails = []
    print('Parsing time detail from ' + timeDetailFname + '...')

    for pageNum in tqdm(range(timeDetailReader.numPages)):
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

    hTimeDetail.close()
    return timeDetails

def pairData(invoices, timeDetail):
    pairedData = []
    print('Pairing invoices and time details...')

    for inv in tqdm(invoices):
        pairedTimeDetail = None
        for td in timeDetail:
            if inv.projectNum == td.projectNum:
                pairedTimeDetail = td
        if pairedTimeDetail == None:
            print('Couldn\'t find matching time detail for invoice!')
            print('Page ' + str(inv.pageNum + 1) + ': Invoice ' + inv.invoiceNum + ' for ' + inv.projectNum + ' ' + inv.projectName)
        else:
            pairedData.append(PairedData(inv, pairedTimeDetail))

    return pairedData

def mergePairs(pairedData, invoiceFname, timeDetailFname, outputDir):
    invoiceReader = PyPDF2.PdfFileReader(invoiceFname)
    timeDetailReader = PyPDF2.PdfFileReader(timeDetailFname)
    
    print('Merging pairs to disk...')
    for pair in tqdm(pairedData):
        invoicePage = pair.invoice.pageNum
        timeDetailPages = pair.timeDetail.pages
        pdfWriter = PyPDF2.PdfFileWriter()
        pdfWriter.addPage(invoiceReader.getPage(invoicePage))
    
        for p in timeDetailPages:
            pdfWriter.addPage(timeDetailReader.getPage(p))
        with Path(outputDir + '/ASM_AN_' + pair.invoice.projectNum + '_' + pair.invoice.projectName.replace('\\', '.').replace('/', '.') + '_' + str(pair.invoice.invoiceNum) + '.pdf').open('wb') as outputFile:
            pdfWriter.write(outputFile)

def findPropertyNameEntryCandidates(db, pName, fuzzRatio):
    candidates = []
    for wsName in db.ws_names:
        for rowNum, row in enumerate(db.ws(wsName).col(col=3)[2:]):
            if fuzz.ratio(pName, row) >= fuzzRatio:
                candidates.append(db.ws(wsName).row(rowNum + 3))
    return candidates

    bestFullMatch = 0
    bestRatio = 0

def maxFullAndPartial(fuzzRatio, db, pairedData, dbg):
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
        
        if dbg:
            print('Partial match for ' + str(partialMatchRatio * 100) + '% with avg. count ' + str(partialMatchAvg) + ' min ' + str(min(partialMatchCounts)) + ' max ' + str(max(partialMatchCounts)))
            print('Full match for ' + str(fullMatchRatio * 100) + '%')
            print('No match for ' + str(noMatch / len(pairedData) * 100) + '%')
        
        return (fuzzRatio, fullMatched, pairCandidates)
    except:
        return (0, 0, None)

def statisticalAnalysis(dbD, pairedDataD):
    print('Running statistical analysis...')
    pool = mp.Pool(processes=16)
    results = pool.map(partial(maxFullAndPartial, db=dbD, pairedData=pairedDataD, dbg=False), range(30, 72, 2))
    bestPair = (0, 0)
    
    for r in results:
        if bestPair[1] < r[1]:
            bestPair = r
    print('Best ratio: ' + str(bestPair[0]))
    return maxFullAndPartial(bestPair[0], db=dbD, pairedData=pairedDataD, dbg=True)[2]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Invoice automator')
    parser.add_argument('InvoiceFile', type=str, help='Path to invoice PDF')
    parser.add_argument('TimeDetailFile', type=str, help='Path to time detail PDF')
    parser.add_argument('OutputDirectory', type=str, help='Path to place merged PDFs')
    args = parser.parse_args()

    if not os.path.isfile(args.InvoiceFile):
        print('Invoice file ' + args.InvoiceFile + ' does not exist!')
        os._exit(-1)
    if not os.path.isfile(args.TimeDetailFile):
        print('Time detail file ' + args.TimeDetailFile + ' does not exist!')
        os._exit(-1)
    if not os.path.isdir(args.OutputDirectory):
        print('Output directory ' + args.OutputDirectory + ' does not exist!')
        os._exit(-1)

    invoiceFname = args.InvoiceFile
    timeDetailFname = args.TimeDetailFile
    assetAssignmentsFname = 'asset_assignments.xlsx'

    invoices = extractInvoices(invoiceFname)
    timeDetail = extractTimeDetail(timeDetailFname)
    pairedData = pairData(invoices, timeDetail)

    #mergePairs(pairedData, invoiceFname, timeDetailFname, args.OutputDirectory)

    #print('Done. I love you Mom!')


    db = xl.readxl(fn=assetAssignmentsFname)
    pairCandidates = statisticalAnalysis(db, pairedData)
    #print(pairCandidates)
