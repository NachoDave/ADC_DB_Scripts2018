# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 14:53:40 2018

@author: dave
"""
import requests # library to access url
#import matplotlib #
#import map_retrieve as mr
#import map_retrieve2 as mr2
import pickle
import time 
#import math
from mysql.connector import connection, errorcode, MySQLConnection, Error
from python_mysql_db_config import read_db_config, connect
#from mySqlInsertEG import insert_cols

from xml.etree import ElementTree as ET

#import re
import proTargetMakeTables as mkTb
import proteinTarTables as tabls
#import json
import time

start = time.time()
''' Get the ENSEMBL Ids =================================================='''
with open('ensId21_11_2018.dat', 'rb') as fff:
    ensUniPrtIds2 = pickle.load(fff)
ensId = ensUniPrtIds2[0][:]
ensId = list(set(ensId))
#uniPrtEnsId = ensUniPrtIds2[1][:] 

''' Connect to database'''

cnx, cur = mkTb.dbconnect( '127.0.0.1', 'root','Four4Legs!Word#Rate0', 'ADC_211118') # connect to DB

''' Make HPA tables in DB =================================================='''

for dx in reversed(tabls.TabNmHPA):
    print('Deleting ' + dx)
    cur.execute("DROP TABLE IF EXISTS " + dx)    

for dx in (tabls.TabNmHPA): # uses the list of tables specified in proteinTarTables.py
    print('Creating table ' + dx)
    cur.execute(tabls.TABLES_HPA[dx])
    
for dx in reversed(tabls.TabNmHPAPath):
    print('Deleting ' + dx)
    cur.execute("DROP TABLE IF EXISTS " + dx)    

for dx in (tabls.TabNmHPAPath): # uses the list of tables specified in proteinTarTables.py
    print('Creating table ' + dx)
    cur.execute(tabls.TABLES_HPA_PATH[dx])

print('Created tables')

''' Collect data from Human protein atlas =================='''

''' Loop through ENSEMBL Ids and get data from HPA'''

enIdCnt = -1

stainLocAr = [] # store the stain locations

for dx in ensId:
    enIdCnt = enIdCnt + 1
    #print(dx)
    # Get all information from protein atlas in xml format   
    x = requests.get('http://www.proteinatlas.org/' + dx + '.xml' )
    # Get the relevent fields from the x.text (which is in xml format)
    
    if x.ok:    
    
        root = ET.fromstring(x.text)
        #f = open('ENSG00000165264.xml')
        #f = open('ENSG00000161653.xml')
        #root = ET.fromstring(f.read())
    
        
        chld = root.find('entry')
        proteinEv = chld.find('proteinEvidence').attrib['evidence']
    #    print(proteinEv)    
        # select healthy tissues ==================================#
        L = len(root.findall('.entry/tissueExpression/data'))  # number of data elements
        tissues = [None]*L# store tissues
        tislevel =  [None]*L
        cellType = [None]*L 
        cellLev = [None]*L   
           
        for ticancSex in root.findall('./entry/tissueExpression'):
            #print(len(ticancSex.findall('.//tissue')))
    
            #print(tissues)
            
            cnt = 0
            for tis, lev in zip(ticancSex.findall('./data/tissue'), ticancSex.findall('./data/level'), \
            ):
    #            print(tis.text, lev.text)
                tissues[cnt] = tis.text
                tislevel[cnt] = lev.text
    
                
                cnt = cnt + 1
            
            # get cell types and levels (note more than one cell type per tissue)
            cnt1 = 0
            for dat in ticancSex.findall('./data'):
                cnt2 = 0
                for clTyp, clLev in zip(dat.findall('./tissueCell/cellType'),\
                                        dat.findall('./tissueCell/level')):
                    tCelTypN = len(dat.findall('./tissueCell/cellType')) 
                    if cnt2 == 0:
                        cellType[cnt1] = [None]*tCelTypN
                        cellLev[cnt1] = [None]*tCelTypN
                        
                    cellType[cnt1][cnt2] = clTyp.text
                    cellLev[cnt1][cnt2] = clLev.text
                    
    #                print(cnt1, cnt2,tissues[cnt1],clTyp.text)
                    cnt2 = cnt2 + 1
                cnt1 = cnt1 + 1
                
#        # get cancer and patient information (this is moved below in the cancer tissue to db loop)
#        cancN = len(root.findall('.entry/antibody/tissueExpression[@assayType="pathology"]/data'))
#        cancerTis = [None]*cancN # array to store cancer names
#        antibod_HPA_Id = [None]*cancN
#        cancSex = [[] for i in range(cancN)] # 2D array to store all patient cancSex
#        cancAge = [[] for i in range(cancN)] # 2D array to store all patient cancAge
#        patientId = [[] for i in range(cancN)] # 2D array to store all patient Ids
#        cancLevStain = [[] for i in range(cancN)] # 2D array to store all staining level
#        cancLevIntens = [[] for i in range(cancN)] # 2D array to store all intensity level
#        cancQuantity = [[] for i in range(cancN)] # 2D array to store all staining level    
#        cancLoc = [[] for i in range(cancN)]
#        imageURL = [[] for i in range(cancN)]
#        
#        cCnt = 0 # cancer loop counter     
#        #for canc in root.findall('.entry/antibody/tissueExpression[@assayType="pathology"]/data'):
#        for antibodId in root.findall('.entry/antibody'): # loop through different antibodies
#            for canc in antibodId.findall('./tissueExpression[@assayType="pathology"]/data'):
#                ptCnt = -1 # counter for the next patient
#                for el in canc.getchildren():
#                    if el.tag == 'tissue': # find the cancerous tissue and all it children
#                        #print(el.text)
#                        cancerTis[cCnt] = el.text
#                        antibod_HPA_Id[cCnt] = antibodId.attrib['id']
#                        ptCnt = ptCnt + 1
#                        
#                        
#                    if el.tag == 'patient': # find the patient info and children
#                        cancSex[cCnt].append(el.find('sex').text)
#                        cancAge[cCnt].append(el.find('age').text)
#                        patientId[cCnt].append(el.find('patientId').text)
#                        cancLevStain[cCnt].append(el.find('level[@type="staining"]').text)
#                        cancLevIntens[cCnt].append(el.find('level[@type="intensity"]').text)
#                        cancQuantity[cCnt].append(el.find('quantity').text)
#                        cancLoc[cCnt].append(el.find('location').text)
#                        imageURL[cCnt].append(el.find('./sample/assayImage/image/imageUrl').text)
#                        #print(el.find('cancSex').text)
#                        
#                cCnt = cCnt + 1
        # select parameters from pathology ===i=====================#
        #print(tissues)    
        ''' Change tissues where field is duplicated =========================== '''
        
        
        if tissues.count('endometrium') > 1:
            tissues[tissues.index('endometrium')] = tissues[tissues.index('endometrium')] + '1'
            tissues[tissues.index('endometrium')] = tissues[tissues.index('endometrium')] + '2'
        
        if tissues.count('skin') > 1:
            tissues[tissues.index('skin')] = tissues[tissues.index('skin')] + '1'
            tissues[tissues.index('skin')] = tissues[tissues.index('skin')] + '2'    
        
        if tissues.count('soft tissue') > 1:
            tissues[tissues.index('soft tissue')] = tissues[tissues.index('soft tissue')] + '1'
            tissues[tissues.index('soft tissue')] = tissues[tissues.index('soft tissue')] + '2'
        
        if tissues.count('stomach') > 1:
            tissues[tissues.index('stomach')] = tissues[tissues.index('stomach')] + '1'
            tissues[tissues.index('stomach')] = tissues[tissues.index('stomach')] + '2'    
        # Write to database 
        
    
        """ HPA data to db, Healthy tables ====================================="""
        if root.findall('./entry/tissueExpression'):    
            tabDx = -1
            ''' Cell type table ===================================================='''
            ''' Tissues table and ENSEMBL_Tissue Junction_Table ===================='''
        # Note probably only need to do this the first time round
            tabDx = tabDx + 1
            for ix, jx in zip(tissues, tislevel):
                cur.execute("INSERT IGNORE INTO "+tabls.TabNmHPA[tabDx]+"(TissueName) VALUES(%s)", (ix,))
                cur.execute("SELECT Id FROM "+tabls.TabNmHPA[tabDx]+" WHERE TissueName LIKE %s ", (ix, ))
                tisId = cur.fetchone()[0]
            #print(tisDx)
                cur.execute("INSERT INTO " + tabls.TabNmHPA[tabDx + 1] +"(ENSEMBL_Gene_Id, HealthyTissueLevel, Tissue_Id) VALUES(%s, %s, %s)", (dx, jx, tisId))    
        
        
            ''' CellType table -----------------------------------------------------'''
            tabDx = tabDx + 2
            cellTypesDwn = list(set([item for sublist in cellType for item in sublist]))
            for ix in cellTypesDwn:
                cur.execute("INSERT IGNORE INTO " + tabls.TabNmHPA[tabDx]+"(CellType) VALUES(%s)", (ix,))
        
        
            ''' ENSEMBL TISSUE CELL junction table ---------------------------------------'''
            tabDx = tabDx + 1 
            for ix, jx, kx in zip(tissues, cellType, cellLev):  # loops through tissues and the first level of the cell arrays
                cur.execute("SELECT Id FROM "+tabls.TabNmHPA[tabDx - 3]+" WHERE TissueName LIKE %s ", (ix, ))
                tisId = cur.fetchone()[0]
    
                for celx, clevx in zip(jx, kx):
                    cur.execute("SELECT Id FROM "+tabls.TabNmHPA[tabDx - 1]+" WHERE CellType LIKE %s ", (celx,))
                    celTyp = cur.fetchone()[0]
                    cur.execute("INSERT INTO " + tabls.TabNmHPA[tabDx] + "(ENSEMBL_Gene_Id, Tissue_Id, CellType_Id, HealthyCellLevel) \
                    VALUES(%s, %s, %s, %s)", (dx, tisId, celTyp, clevx))
    
        """ HPA data to db, cancer tables ====================================="""
        if root.findall('.entry/antibody/tissueExpression[@assayType="pathology"]/data'): # only do if pathology data
            tabDx2 = -1
            tabDx2 = tabDx2 + 1
            ''' Cancer tissue and stain level table -----------------------------------------------'''
        
             # get cancer and patient information

            #for canc in root.findall('.entry/antibody/tissueExpression[@assayType="pathology"]/data'):
            for antibodId in root.findall('.entry/antibody'): # loop through different antibodies
                antibod_HPA_Id = antibodId.attrib['id'] # HPA antibody ID            
                
                #cancN = len(root.findall('./tissueExpression[@assayType="pathology"]/data'))
                cancN = len(antibodId.findall('./tissueExpression[@assayType="pathology"]/data'))
                cancerTis = [None]*cancN # array to store cancer names
                
                cancSex = [[] for i in range(cancN)] # 2D array to store all patient cancSex
                cancAge = [[] for i in range(cancN)] # 2D array to store all patient cancAge
                patientId = [[] for i in range(cancN)] # 2D array to store all patient Ids
                cancLevStain = [[] for i in range(cancN)] # 2D array to store all staining level
                cancLevIntens = [[] for i in range(cancN)] # 2D array to store all intensity level
                cancQuantity = [[] for i in range(cancN)] # 2D array to store all staining level    
                cancLoc = [[] for i in range(cancN)]
                imageURL = [[] for i in range(cancN)]            
                
                cCnt = 0 # cancer loop counter  
                for canc in antibodId.findall('./tissueExpression[@assayType="pathology"]/data'): 
                    ptCnt = -1 # counter for the next patient
                    
                    for el in canc.getchildren():
                        if el.tag == 'tissue': # find the cancerous tissue and all it children
                            #print(el.text)
                            cancerTis[cCnt] = el.text
                            
                            ptCnt = ptCnt + 1
                            
                            
                        if el.tag == 'patient': # find the patient info and children
                            cancSex[cCnt].append(el.find('sex').text)
                            cancAge[cCnt].append(el.find('age').text)
                            patientId[cCnt].append(el.find('patientId').text)
                            cancLevStain[cCnt].append(el.find('level[@type="staining"]').text)
                            cancLevIntens[cCnt].append(el.find('level[@type="intensity"]').text)
                            cancQuantity[cCnt].append(el.find('quantity').text)
                            cancLoc[cCnt].append(el.find('location').text)
                            imageURL[cCnt].append(el.find('./sample/assayImage/image/imageUrl').text)
                            #print(el.find('cancSex').text)
                            
                    cCnt = cCnt + 1            
                #print(cancerTis, '\n\n', antibod_HPA_Id, '\n\n', locId)
                # loop through cancers
                for ix, jx, cancSexx, patIdx, locDx, levIntDx, stainDx, quantDx, ageDx, imDx \
                in zip(cancerTis, cancLevStain, cancSex, patientId, cancLoc, cancLevIntens, cancLevStain, cancQuantity, cancAge, imageURL):
                    #print(ix)
                    cur.execute("INSERT IGNORE INTO " + tabls.TabNmHPAPath[tabDx2] + "(TissueName) VALUES(%s)", (ix, )) # add tissue if not already present
                    cur.execute("SELECT Id FROM "+tabls.TabNmHPAPath[tabDx2]+" WHERE TissueName LIKE %s ", (ix, ))
                    tisId = cur.fetchone()[0]
                    
                    #print(ix, tisId)
                    
                    ''' Stain levels table -----------------------------------------'''
                    maleND_L_M_H = [0]*4  # count the number of 
                    femaleND_L_M_H = [0]*4
                    
                    
                    # loop through patients
                    for iddx, levx, sx, locDxx, levIntDxx, stainDxx, quantDxx, ageDxx, imDxx  \
                    in zip(patIdx, jx, cancSexx, locDx, levIntDx, stainDx, quantDx, ageDx, imDx  ): # insert into CancerTissuePateint_HPA
                        
    #                    # Check if there are anymore stain locations added and append to array storing them
    #
    #                    if locDxx not in stainLocAr:
    #                        stainLocAr.append(locDxx)
                        
                        # Cancer stain location
                        cur.execute("INSERT IGNORE INTO " + tabls.TabNmHPAPath[tabDx2 + 1] + "(StainLocation) VALUES(%s)", (locDxx, )) # add tissue if not already present
                        cur.execute("SELECT Id FROM "+tabls.TabNmHPAPath[tabDx2 + 1]+" WHERE StainLocation LIKE %s ", (locDxx, ))
                        locId = cur.fetchone()[0]                
    
                        # Cancer Patients table
                        cur.execute("INSERT IGNORE INTO " + tabls.TabNmHPAPath[tabDx2 + 3] + \
                        "(ENSEMBL_Gene_Id, CancTissue_Id, PatientId, Sex, StainLoc_Id, Intensity, StainLevel, Quantity, Age, ImageURL, Antigen_HPA_Id) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                        (dx, tisId, int(iddx), sx, locId, levIntDxx , stainDxx, quantDxx, ageDxx, imDxx, antibod_HPA_Id )) # add patient ID
                
                           
                
                        if sx == 'Male' and stainDxx == 'Not detected':
                            maleND_L_M_H[0] = maleND_L_M_H[0] + 1
                        elif sx == 'Male' and stainDxx == 'Low':
                            maleND_L_M_H[1] = maleND_L_M_H[1] + 1
                        elif sx == 'Male' and stainDxx == 'Medium':
                            maleND_L_M_H[2] = maleND_L_M_H[2] + 1
                        elif sx == 'Male' and stainDxx == 'High':
                            maleND_L_M_H[3] = maleND_L_M_H[3] + 1
                
                        if sx == 'Female' and stainDxx == 'Not detected':
                            femaleND_L_M_H[0] = femaleND_L_M_H[0] + 1
                        elif sx == 'Female' and stainDxx == 'Low':
                            femaleND_L_M_H[1] = femaleND_L_M_H[1] + 1
                        elif sx == 'Female' and stainDxx == 'Medium':
                            femaleND_L_M_H[2] = femaleND_L_M_H[2] + 1
                        elif sx == 'Female' and stainDxx == 'High':
                            femaleND_L_M_H[3] = femaleND_L_M_H[3] + 1
                            
                    # Cancer tissues level
                    cur.execute("INSERT INTO " + tabls.TabNmHPAPath[tabDx2 + 2] + \
                    "(ENSEMBL_Gene_Id, CancTissue_Id, Antigen_HPA_Id, LevelNotDetectedM, LevelLowM, LevelMediumM, LevelHighM, LevelNotDetectedF, LevelLowF, LevelMediumF, LevelHighF) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ", \
                    (dx, tisId, antibod_HPA_Id,  maleND_L_M_H[0], maleND_L_M_H[1], maleND_L_M_H[2], maleND_L_M_H[3], femaleND_L_M_H[0], femaleND_L_M_H[1], femaleND_L_M_H[2], femaleND_L_M_H[3]))                    
    
    if not enIdCnt%100:
        print(dx)
        print('Commiting ' + str(enIdCnt - 100) + ':' + str(enIdCnt))
        cnx.commit()


#    if enIdCnt > 1000:
#        break
    
cur.close()
cnx.close()    
end = time.time()
print(end - start)