# -*- coding: utf-8 -*-
"""
Multivariate Hotelling-T Sq based Anomaly Detection

Libraries Needed:
    numpy
    pandas
    sklearn

INPUTS:
    Call using function call to CallCodeIndSc(rawdata, windowPrev, windowFut, HTh)
    Data should be passed a Python Data Frame
    Parameters: 
        rawdata    : Python Data Frame, with First Column being TimeStamp and called as 'TIME'
        windowPrev : sliding window for sample I. Eg. 10
        windowFut  : sliding window to sample II. Eg. 5
        HTh        : threshold for alert cut-off
    Output:
        ThValAlertH: Python Data Frame with only the alert times as 'TIME' and score values 'TestScores'
                     Use the TIME values for subsequent alarm points
        For testing purpose, this code also writes various *.xlsx files. These can be suppressed if needed.

@author: Subrat Nanda
"""
import numpy as np
import pandas as pd
from sklearn import preprocessing

smallPoNum = 0.0001

def CallCodeIndSc(rawdata, windowPrev, windowFut, HTh):
    rawdataH = rawdata.drop('DateTime', axis = 1)
    IndValsF = GetInd_AD2(rawdataH, windowPrev, windowFut)
    NumIter = IndValsF.shape
    ColNamesH = ['TestScore']
    TestVals = pd.DataFrame(columns = ColNamesH)
    for iIter in range(0, NumIter[0]):
        IndexHere = IndValsF.iloc[iIter]        
        if iIter > 0:
            PrevSeries = rawdataH.iloc[IndexHere['startIndPrev']:IndexHere['endIndPrev']]
        else:
            PrevSeries = rawdataH.iloc[0]            
        FutSeries = rawdataH.iloc[IndexHere['startIndFut']:IndexHere['endIndFut']]
        PrevSeries2 = np.array(PrevSeries)
        FutSeries2 = np.array(FutSeries)
        try:
            TestHere = construct_anomaly(PrevSeries2, FutSeries2)
        except: 
            TestHere = smallPoNum   
        TestHere = pd.Series(TestHere, index=ColNamesH)
        TestVals =  TestVals.append(TestHere,ignore_index=True)
    ThVals = TestVals >= HTh
    #print(ThVals)
    AllDataWscr = pd.concat([rawdata,ThVals, TestVals], axis=1)
#    print(AllDataWscr)
    ThValAlert = AllDataWscr[ThVals['TestScore'] != False]
#    ThValAlert.loc[:,('DateTime', 'TestScore')].to_excel('HADResults.xlsx', sheet_name = 'AlertTime')
    ThValAlertH = ThValAlert.loc[:,('DateTime', 'TestScore')]
    return(ThValAlertH)
    
    
""" Getting Indexes """


def GetInd_AD2(rawdata, windowPrev, windowFut):
    dataSize = rawdata.shape
    #rawdataVal = rawdata.drop('TIME', axis = 1)
    rawdataVal = rawdata
    dataSize2 = rawdataVal.shape
    ColNames = ['startIndPrev', 'endIndPrev', 'startIndFut', 'endIndFut']
    IndVals = pd.DataFrame(columns = ColNames)
    for iRow in range(0, dataSize2[0]):
        if (iRow <= windowPrev) & (iRow + windowFut <= dataSize2[0]):
            startIndPrev = 0
            endIndPrev = iRow
            startIndFut = iRow + 1
            endIndFut = iRow + windowFut
            tempVals = pd.Series([startIndPrev, endIndPrev, startIndFut, endIndFut], index=ColNames)
            IndVals = IndVals.append(tempVals, ignore_index=True)
        elif (iRow > windowPrev) & (iRow + windowFut <= dataSize2[0]):
            startIndPrev = iRow - windowPrev + 1
            endIndPrev = iRow
            startIndFut = iRow + 1
            endIndFut = iRow + windowFut
            tempVals = pd.Series([startIndPrev, endIndPrev, startIndFut, endIndFut], index=ColNames)
            IndVals = IndVals.append(tempVals, ignore_index=True)
        elif (iRow > windowPrev) & (iRow + windowFut > dataSize2[0]):
            startIndPrev = iRow - windowPrev + 1
            endIndPrev = iRow
            startIndFut = iRow
            endIndFut = dataSize2[0]
            tempVals = pd.Series([startIndPrev, endIndPrev, startIndFut, endIndFut], index=ColNames)
            IndVals = IndVals.append(tempVals, ignore_index=True)
            
    return(IndVals)   


""" Getting Score """


def construct_anomaly(prevSeries, futSeries):
    if prevSeries.ndim == 1:
        normPrev = prevSeries.reshape(1,-1)        
    else:
        normPrev = normalize(prevSeries)
   
    meanPrev = np.mean(normPrev, axis = 0)
    
    if futSeries.ndim == 1:
        normFut = futSeries.reshape(1,-1)        
    else:
        normFut = normalize(futSeries)       
    
    meanFut = np.mean(normFut, axis = 0)
    covHist = np.cov(normPrev, rowvar=False)
    invCovHist = np.linalg.inv(covHist)
    intmDiff = meanFut - meanPrev
    TestScore = intmDiff @ invCovHist @ intmDiff.T
    return(TestScore)

""" Normalize """


def normalize(input_mat):
    minmax_scale = preprocessing.MinMaxScaler(feature_range = (0,1))
    scaled_feature = minmax_scale.fit_transform(input_mat)
    return(scaled_feature)



# Call Function
    #GetInd_AD2(data, 20, 6)