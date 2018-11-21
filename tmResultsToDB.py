# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 15:47:24 2018

@author: dave
"""

''' TM results to DB ======================================================'''

import readTMResults as TMrs
import proteinTarTables as tabls
import proTargetMakeTables as mkTb

'''Routine for parsing topcons data======================================= '''
pth = '/media/dave/Seagate Backup Plus Drive/Bioinformatics/TransmembraneTools/TopConsResults/FirstAttempt_Protein0to39999_270918/rst_6ea8vY/seq_'
fn = '/query.result.txt'

#fnum = [1145]
fnum = [10096, 1145, 10460]

""" Connect to DB =========================================================="""
cnx, cur = mkTb.dbconnect( '127.0.0.1', 'root','FourLegsWordRate', 'ADC_Targeter') # connect to DB
""" Create TM tables ======================================================="""

for dx in reversed(tabls.sofTabNms):
    print('Deleting ' + dx)
    cur.execute("DROP TABLE IF EXISTS " + dx)    

for dx in (tabls.sofTabNms): # uses the list of tables specified in proteinTarTables.py
    print('Creating table ' + dx)
    cur.execute(tabls.TABLES_SOFT[dx])

""" Write software names =================================================="""

softwares = ['Topcons', 'Spoctopus', 'Octopus', 'Philius', 'Scampi', 'PolyPhobius']

for dx in softwares:
    cur.execute("INSERT IGNORE INTO " + tabls.sofTabNms[0] + "(SoftwareName) VALUES(%s)", (dx, ))


""" Loop through topcons results (includes Spoctopus, Octopus, philius, scampi, phobius) """

for fndx in fnum: # loop through Proteins
    
    pthf = pth + str(fndx)
    f1 = TMrs.topconRes(pthf, fn) # inputs 

    f1.runAll()
    """ Write to DB =========================================================== """
    if f1.noTMPreds: # only append if TM regions presnet
        tabDx = 0
       
        """ Write to summary table -------------------------------------------- """
        
        tabDx = tabDx + 1
        
        maxOLen = max(f1.topConOLens + f1.octOLens + f1.spoctOLens + f1.philOLens \
        + f1.polPhoOLens + f1.scampiOLens)# get the longest predicted extracellular region
        
        minOLen = min(f1.topConOLens + f1.octOLens + f1.spoctOLens + f1.philOLens \
        + f1.polPhoOLens + f1.scampiOLens) # get shortest predicted extracellular region   
        
        cur.execute("SELECT Id FROM UniProt_Proteins WHERE Accession LIKE %s", (f1.gnNm, ))  
        upId = cur.fetchone()[0] # get the uniprot Id
        
        cur.execute("INSERT IGNORE INTO " + tabls.sofTabNms[tabDx] + "(UniprotId,  "
        "TopconsTM_N, OctopusTM_N, SpoctopusTM_N, PhiliusTM_N, ScampiTM_N, PolyPhobiusTM_N,"
        "TopconsO_N, OctopusO_N, SpoctopusO_N, PhiliusO_N, ScampiO_N, PolyPhobiusO_N,"
        "LongestO, ShortestO ) "
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
        (upId, f1.topConTMN, f1.octTMN, \
        f1.octTMN, f1.philTMN, f1.scampiTMN, f1.polPhoTMN, \
        f1.topConON, f1.octON, f1.octON, f1.philON, f1.scampiON, f1.polPhoON, \
        maxOLen, minOLen)) 
    
""" Disconnect to DB ======================================================="""    
cnx.commit()
cur.close()
cnx.close()