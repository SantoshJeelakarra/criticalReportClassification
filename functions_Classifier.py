#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  9 22:56:41 2019
Functions needed for classifer script

@author: santosh
"""

import pandas as pd
import mysql.connector
import numpy as np

# establishing connection to database
mydb = mysql.connector.connect(host="livehealth-may.crix9ruu2jlz.ap-south-1.rds.amazonaws.com", user="livehealth", passwd="LiveHealth2013",database="livehealthapp")

mycursor = mydb.cursor()


def gatherData(lRRID):
    """This function gathers data from different tables based on the labReportID"""
    
    #extract lRR data from labReportRelation
    lrr = pd.read_sql("select * from labReportRelation where labReportId = %s and completedTests = 1 and reportFormatId_id is not null",mydb,params=[str(lRRID)])
    
    #extract userId from lRR
    userId=lrr.loc[0,'userDetailsId_id']
    
    #extract gender and age based on the userId
    sql1=pd.read_sql("select sex,ageInDays from userDetails where id = %s",mydb,params=[str(userId)])
    Gendr=sql1.iat[0,0][0:1]
    Age=sql1.iat[0,1]
    Age=Age/365
    
    #creating blank dataframes
    ReportFormat1=pd.DataFrame()
    ReportValues1=pd.DataFrame()
    
    #extract ReportFormatId from lRR
    ReportFormatId=lrr.loc[0,"reportFormatId_id"]
    ReportFormat=pd.read_sql("select * from reportFormat where reportFormatforId= %s and concat(lowerBoundFemale,lowerBoundMale,upperBoundMale,upperBoundFemale) not regexp '-|''|,' and concat(lowerBoundFemale,lowerBoundMale,upperBoundMale,upperBoundFemale) not like '' and isDisable=0 and isGraph=0 and descriptionFlag=0 and listField=0 and styleFlag=0 and fileInput=0 and concat(lowerBoundFemale,lowerBoundMale,upperBoundMale,upperBoundFemale) not regexp '[a-zA-Z<>`%\+\/]' and concat(lowerBoundFemale,lowerBoundMale,upperBoundMale,upperBoundFemale) not regexp '^[.]' and concat(lowerBoundFemale,lowerBoundMale,upperBoundMale,upperBoundFemale) not regexp '[.]$' and concat(lowerBoundFemale,lowerBoundMale,upperBoundMale,upperBoundFemale) not regexp '[.]000$' ",mydb,params=[str(ReportFormatId)])
    ReportFormat1=pd.concat([ReportFormat1,ReportFormat])

    #Extract ValueRange data from valueRanges table
    valueRange=pd.read_sql("select * from valueRanges where reportFormatRelationID_id = %s",mydb,params=[str(ReportFormatId)])
    
    #Retrieve reportValues from table
    ReportValues=pd.read_sql("select * from reportValues where reportForId_id=%s",mydb,params=[str(lRRID)])
    ReportValues1=pd.concat([ReportValues1,ReportValues])
    ReportValues1=ReportValues1[~(ReportValues1['value']=='-')]
    ReportValues1['ValidValue']=""
    
    # to extract the numeric records from the ReportValues
    ReportValues_numeric=ReportValues1[pd.to_numeric(ReportValues1["value"],errors='coerce').notnull()]

    # to extract the non-numeric records from the ReportValues
    ReportValues_nonNumeric=ReportValues1[pd.to_numeric(ReportValues1["value"],errors='coerce').isnull()]
    
    #create a blank column in both ReportFormat and ValueRange columns for saving "Range Not Defined" information
    ReportFormat1['RangeDefined']=""
    valueRange['RangeDefined']=""
    
    #create a blank column in both ReportFormat and ValueRange columns for saving "CriticalRange Not Defined" information
    ReportFormat1['CriticalRangeDefined_RepFormat']=""
    ReportFormat1['CriticalRangeDefined_ValueRange']=""

    
    #setup index for valueRange dataframe
    valueRange.index=np.arange(0,len(valueRange))
    
    return (lrr,userId,Gendr,Age,ReportFormatId,ReportFormat1,ReportValues_numeric,ReportValues_nonNumeric,valueRange)

# ------------------------------------------------------------------------------------------------------------------------ #
    
def rangeCheck(RepFormat,Gendr):
    """ This function checks if the Normal Range is defined or not in the ReportFormat"""
    for Rformat in RepFormat.itertuples():
        if Rformat.ageRangeFlag==0 and Gendr=='M':
            if Rformat.lowerBoundMale=='-' and Rformat.upperBoundMale=='-':
                RepFormat.at[Rformat.Index,'RangeDefined']=0
            else:
                RepFormat.at[Rformat.Index,'RangeDefined']=1

# ------------------------------------------------------------------------------------------------------------------------ #
                

def criticalRange(RepFormat,ReportValues_numeric,Gendr):
    """This function is to check if the Critical Ranges are defined or not in the ReportFormat"""
    for Rvalue in ReportValues_numeric.itertuples():
        for Rformat in RepFormat.itertuples():
            if Rformat.indexCol==Rvalue.indexCol:
                if Rformat.ageRangeFlag==0 and Gendr=='M':
                    if Rformat.criticalLowerMale=='-' and Rformat.criticalUpperMale=='-':
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=0
                    else:
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=1
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=0
                elif Rformat.ageRangeFlag==0 and Gendr=='F':
                    if Rformat.criticalLowerFemale=='-' and Rformat.criticalUpperFemale=='-':
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=0
                    else:
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=1
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=0

# ------------------------------------------------------------------------------------------------------------------------ #


def ageRangeCheck(RepFormat,valueRange):
    """ This function is to check if the Range is available based on the Age"""
    for ind,row in RepFormat.iterrows():
        if RepFormat.loc[ind,'ageRangeFlag'] == 1:
            indx=RepFormat.loc[ind,'indexCol']
            if any(valueRange.indexCol== indx):
                RepFormat.loc[ind,'RangeDefined']=1
                continue
            else:
                RepFormat.loc[ind,'RangeDefined']=0
                RepFormat.loc[ind,'CriticalRangeDefined_ValueRange']=0
                print("No Age Range")

# ------------------------------------------------------------------------------------------------------------------------ #


def valueRangeCopy(RepFormat,valueRange,Age):
    """This function is to copy the Normal Ranges from ValueRanges table to ReportFormat table for ease of calculations"""
    for Rformat in RepFormat.itertuples():
        for valRange in valueRange.itertuples():
            if Rformat.ageRangeFlag==1 and Rformat.RangeDefined==1 and Rformat.indexCol==valRange.indexCol:
                if Age>valRange.lowerAge and Age<valRange.upperAge:
                    RepFormat.at[Rformat.Index,'upperBoundMale']=valueRange.at[valRange.Index,'upperBoundMale']
                    RepFormat.at[Rformat.Index,'lowerBoundMale']=valueRange.at[valRange.Index,'lowerBoundMale']
                    RepFormat.at[Rformat.Index,'upperBoundFemale']=valueRange.at[valRange.Index,'upperBoundFemale']
                    RepFormat.at[Rformat.Index,'lowerBoundFemale']=valueRange.at[valRange.Index,'lowerBoundFemale']

# ------------------------------------------------------------------------------------------------------------------------ #

def checkInvalidValues(ReportValues_numeric):
    """This function checks for invalid values in the ReportValues"""
    for Rvalue in ReportValues_numeric.itertuples():
        if Rvalue.value.startswith('-'):
            ReportValues_numeric.at[Rvalue.Index,'ValidValue']=0
        else:
            ReportValues_numeric.at[Rvalue.Index,'ValidValue']=1


# ------------------------------------------------------------------------------------------------------------------------ #               

def criticalRangeAgeSpecific(RepFormat,valueRange,Age,Gendr):
    """ This function checks if the critical ranges are defined for ReportFormats with AgeRangeFlag set to 1
        and also checks if the critical ranges are not defined in valueRanges table, check if they are defined in the ReportFormat """
    for Rformat in RepFormat.itertuples():
        for valRange in valueRange.itertuples():
            if Rformat.ageRangeFlag==1 and Rformat.RangeDefined==1 and Rformat.indexCol==valRange.indexCol:
                if Age>valRange.lowerAge and Age<valRange.upperAge and Gendr=='M':
                    print(valRange.lowerAge)
                    if valRange.criticalLowerMale=='-' and valRange.criticalUpperMale=='-':
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=0
                        if Rformat.criticalLowerMale=='-' and Rformat.criticalUpperMale=='-':
                            RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                        else:
                            RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=1
                    else: 
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=1
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                        # below we are copying critical Ranges from ValueRange table to RepFormat table for ease of calculation.
                        RepFormat.at[Rformat.Index,'criticalLowerMale']=valueRange.at[valRange.Index,'criticalLowerMale']
                        RepFormat.at[Rformat.Index,'criticalLowerFemale']=valueRange.at[valRange.Index,'criticalLowerFemale']
                        RepFormat.at[Rformat.Index,'criticalUpperFemale']=valueRange.at[valRange.Index,'criticalUpperFemale']
                        RepFormat.at[Rformat.Index,'criticalUpperMale']=valueRange.at[valRange.Index,'criticalUpperMale']
                elif Age>valRange.lowerAge and Age<valRange.upperAge and Gendr=='F':
                    if valRange.criticalLowerFemale=='-' and valRange.criticalUpperFemale=='-':
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=0
                        if Rformat.criticalLowerFemale=='-' and Rformat.criticalUpperFemale=='-':
                            RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                        else:
                            RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=1
                    else:
                        RepFormat.at[Rformat.Index,'CriticalRangeDefined_ValueRange']=1
            elif Rformat.ageRangeFlag==1 and Rformat.RangeDefined==0:
                if Gendr=='M' and Rformat.criticalLowerMale=='-' and Rformat.criticalUpperMale=='-':
                    RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                elif Gendr=='F' and Rformat.criticalLowerFemale=='-':
                    RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=0
                else:
                    RepFormat.at[Rformat.Index,'CriticalRangeDefined_RepFormat']=1
                
# ------------------------------------------------------------------------------------------------------------------------ #

def emptyRepFormatAndRepValuesCheck(ReportValues_numeric,RepFormat):
    if (ReportValues_numeric.empty or RepFormat.empty):
        print("Either ReportFormat is empty or there is no numeric value to analyze")
        True
    else:
        False
        print("RepFormat/RepValues is not empty")
        

# ------------------------------------------------------------------------------------------------------------------------ #
        
        
def criticalRangeDefinedValuesComparision(ReportValues_numeric,RepFormat,Gendr):
    cols=['TestID','ReportID','indexCol','value','IsCritical','IsAbnormal','IsNormal','DictionaryID']
    lst=[]
    for Rvalue in ReportValues_numeric.itertuples():
        for Rformat in RepFormat.itertuples():
            critical,abnormal,normal=0,0,0
            if Rformat.CriticalRangeDefined_ValueRange==1 or Rformat.CriticalRangeDefined_RepFormat==1:
                if Rvalue.indexCol==Rformat.indexCol:
                    if Gendr=='M':
                        if Rvalue.value < float(Rformat.criticalLowerMale) or Rvalue.value > float(Rformat.criticalUpperMale):
                            critical=1
                        elif (Rvalue.value < float(Rformat.lowerBoundMale) and Rvalue.value> float(Rformat.criticalLowerMale)) or (Rvalue.value > float(Rformat.upperBoundMale) and Rvalue.value < float(Rformat.criticalUpperMale)):
                            abnormal=1
                        elif (Rvalue.value > float(Rformat.lowerBoundMale) and Rvalue.value < float(Rformat.upperBoundMale)):
                            normal=1
                    elif Gendr=='F':
                        if Rvalue.value < float(Rformat.criticalLowerFemale) or Rvalue.value > float(Rformat.criticalUpperFemale):
                            critical=1
                        elif (Rvalue.value < float(Rformat.lowerBoundFemale) and Rvalue.value> float(Rformat.criticalLowerFemale)) or (Rvalue.value > float(Rformat.upperBoundFemale) and Rvalue.value < float(Rformat.criticalUpperFemale)):
                            abnormal=1
                        elif (Rvalue.value > float(Rformat.lowerBoundFemale) and Rvalue.value < float(Rformat.upperBoundFemale)):
                            normal=1
                    lst.append([Rformat.reportFormatforId,Rvalue.reportForId_id,Rvalue.indexCol,Rvalue.value,critical,abnormal,normal,Rformat.dictionaryId_id])
                
    DF=pd.DataFrame(lst,columns=cols)
    return DF

# ------------------------------------------------------------------------------------------------------------------------ #
                        
def criticalRangeNotDefined(ReportValues_numeric,RepFormat,Gendr):
    cols=['TestID','ReportID','indexCol','value','IsCritical','IsAbnormal','IsNormal','DictionaryID']
    lst=[]
    for Rvalue in ReportValues_numeric.itertuples():
        for Rformat in RepFormat.itertuples():
            critical,abnormal,normal=0,0,0
            if not (Rformat.CriticalRangeDefined_ValueRange==1 or Rformat.CriticalRangeDefined_RepFormat==1):
                if Rformat.RangeDefined==1 and Rvalue.indexCol==Rformat.indexCol:
                    if Gendr=='M':
                        if Rvalue.value > float(Rformat.lowerBoundMale) and Rvalue.value < float(Rformat.upperBoundMale):
                            normal=1
                        else:
                            if Rvalue.value < float(Rformat.lowerBoundMale):
                                perclower=float((float(Rformat.lowerBoundMale)-Rvalue.value)*100/float(Rformat.lowerBoundMale))
                                if (Rformat.lowerBoundMale > 1000 and perclower > 20) or (Rformat.lowerBoundMale < 1000 and perclower > 40):
                                    critical=1
                                else:
                                    abnormal=1
                            elif Rvalue.value > float(Rformat.upperBoundMale):
                                perchigher=float((-float(Rformat.upperBoundMale)+Rvalue.value)*100/float(Rformat.upperBoundMale))
                                if (Rformat.upperBoundMale > 1000 and perchigher > 20) or (Rformat.upperBoundMale < 1000 and perchigher > 40):
                                    critical=1
                                else:
                                    abnormal=1
                    else:
                        if Rvalue.value > float(Rformat.lowerBoundFemale) and Rvalue.value < float(Rformat.upperBoundFemale):
                            normal=1
                        else:
                            if Rvalue.value < float(Rformat.lowerBoundFemale):
                                perclower=float((float(Rformat.lowerBoundFemale)-Rvalue.value)*100/float(Rformat.lowerBoundFemale))
                                if (Rformat.lowerBoundFemale > 1000 and perclower > 20) or (Rformat.lowerBoundFemale < 1000 and perclower > 40):
                                    critical=1
                                else:
                                    abnormal=1
                            elif Rvalue.value > float(Rformat.upperBoundFemale):
                                perchigher=float((-float(Rformat.upperBoundFemale)+Rvalue.value)*100/float(Rformat.upperBoundFemale))
                                if (Rformat.upperBoundFemale > 1000 and perchigher > 20) or (Rformat.upperBoundFemale < 1000 and perchigher > 40):
                                    critical=1
                                else:
                                    abnormal=1
                    lst.append([Rformat.reportFormatforId,Rvalue.reportForId_id,Rvalue.indexCol,Rvalue.value,critical,abnormal,normal,Rformat.dictionaryId_id])
    DF=pd.DataFrame(lst,columns=cols)
    return DF

# ------------------------------------------------------------------------------------------------------------------------ #

def finalNumDF():
    comparisionDF=criticalRangeDefinedValuesComparision()
    calcDF=criticalRangeNotDefined()
    frames=[comparisionDF,calcDF]
    finalDF=pd.concat(frames)
    finalDF.index=np.arange(0,len(finalDF))
    
    finalDF['DictionaryID']=pd.to_numeric(finalDF['DictionaryID'])

    for inda,row in finalDF.iterrows():
        if np.isnan(finalDF.loc[inda,'DictionaryID']):
            finalDF.loc[inda,'DictionaryID']=0
        
        
    finalDF.DictionaryID=finalDF.DictionaryID.astype(int)

    return finalDF


# ------------------------------------------------------------------------------------------------------------------------ #          

def createJson():
    final_json=(finalNumDF.groupby(['ReportID'],as_index=True)
    .apply(lambda x:x[['TestID','indexCol','IsCritical','IsAbnormal','IsNormal','DictionaryID']].to_dict('records'))
    .reset_index()
    .rename(columns={0:'Tags'})
    .to_dict(orient='records'))
    
    final_json=pd.DataFrame(final_json)
    
    return final_json

# ------------------------------------------------------------------------------------------------------------------------ #

