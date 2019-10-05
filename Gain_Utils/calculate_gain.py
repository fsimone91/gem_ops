#!/usr/bin/env python3
import numpy
import ROOT as r
import pandas as pd
import numpy as np
import math
import os
import cx_Oracle
import time
from operator import itemgetter
from datetime import datetime
from datetime import timedelta
from gempython.utils.wrappers import envCheck

def environmental_from_DCS(query_time):

    #import DB credentials
    envCheck("GEM_ONLINE_DB_NAME")
    envCheck("GEM_ONLINE_DB_COND")
    dbName = os.getenv("GEM_ONLINE_DB_NAME")
    dbAccount = os.getenv("GEM_ONLINE_DB_COND")

    #time interval considered to extract the data
    start = query_time - timedelta(hours=6)
    end = query_time
    # YYY-MM-dd hh:mm:ss
    sta_period = start.strftime("'%Y-%m-%d %H:%M:%S'")
    end_period = end.strftime("'%Y-%m-%d %H:%M:%S'")
    
    #connection to DB 
    db = cx_Oracle.connect(dbAccount+dbName)
    cur=db.cursor()
    
    # find the dp_id of the raspberryMeasData_BMP280 measurements...
    query_dpid="select DP_ID from dp where dpname like '%rpiPressureQC8'"
    
    cur.execute(query_dpid)
    rasp_id = cur.fetchone()[0]
    #print "raspberry Id = ", rasp_id
    
    #find the dpe_id of the various measurements
    query_dpeid="select DPE_ID,DPENAME from dpe where DP_ID="+str(rasp_id)
    cur.execute(query_dpeid)
    all_id = cur.fetchall()
    
    for result in all_id:
        if (result[1] == "pressureValue_hPa"):
            pres_id=result[0]
        if (result[1] == "temperatureValue_degC"):
            temp_id=result[0]
    #print " T, P dpe_id ",temp_id,pres_id
    
    #Now we catch the element_ID's of the 3 quantities
    query_elid_pres="select ELEMENT_ID from ELEMENTS_ALL where DP_ID="+str(rasp_id)+" and DPE_ID="+str(pres_id)
    cur.execute(query_elid_pres)
    pres_elid = cur.fetchone()[0]
    #print pres_elid
    query_elid_temp="select ELEMENT_ID from ELEMENTS_ALL where DP_ID="+str(rasp_id)+" and DPE_ID="+str(temp_id)
    cur.execute(query_elid_temp)
    temp_elid = cur.fetchone()[0]
    #print temp_elid

    #look for the most recent entry in the db    
    query = "select TS,VALUE_NUMBER from EVENTHISTORY where ELEMENT_ID = "+str(pres_elid)+" and TS > to_date ("+sta_period+",'YYYY-MM-DD HH24:MI:SS') and TS < to_date ("+end_period+",'YYYY-MM-DD HH24:MI:SS') AND TS = (select max(TS) from EVENTHISTORY where ELEMENT_ID = "+str(pres_elid)+" and TS < to_date ("+end_period+",'YYYY-MM-DD HH24:MI:SS') and TS > to_date ("+sta_period+",'YYYY-MM-DD HH24:MI:SS'))"
    
    #print query
    cur.execute(query)
    presmon=cur
    for result in presmon:
        print result[0]," UTC time ",result[1],"hPa pres"
        pressure = result[1]
    
    #look for the most recent entry in the db    
    query = "select TS,VALUE_NUMBER from EVENTHISTORY where ELEMENT_ID = "+str(temp_elid)+" and TS > to_date ("+sta_period+",'YYYY-MM-DD HH24:MI:SS') and TS < to_date ("+end_period+",'YYYY-MM-DD HH24:MI:SS') AND TS = (select max(TS) from EVENTHISTORY where ELEMENT_ID = "+str(temp_elid)+" and TS < to_date ("+end_period+",'YYYY-MM-DD HH24:MI:SS') and TS > to_date ("+sta_period+",'YYYY-MM-DD HH24:MI:SS'))"
    
    #print query
    cur.execute(query)
    tempmon=cur
    for result in tempmon:
        print result[0]," UTC time ",result[1]," celsius temp"
        temperature = 273.15 + result[1]
    
    return temperature,pressure


# Define the parser
import argparse
parser = argparse.ArgumentParser(description="Options to give to calculate_gain.py")
# Positional arguments
parser.add_argument("shelf", type=int, choices=[0, 1, 2], help="Specify shelf number")
parser.add_argument("slot", type=int, choices=[2, 4, 6], help="Specify slot number")
parser.add_argument("gain", type=float, help="Specify effective gas gain value (example 1.5e4)")
# Optional Arguments
parser.add_argument("--temp", type=float, default=23.95, help="Lab. temperature in Celsius for HV setting normalization")
parser.add_argument("--pres", type=float, default=964.4, help="Lab. pressure in mBar for HV setting normalization")
parser.add_argument("--now", default=False, action='store_true', help="Lab. pressure and temp taken from DCS database")
args = parser.parse_args()

if(args.now):
    T,P = environmental_from_DCS(datetime.now())
else:
    T = args.temp + 273.15
    P = args.pres

from gempython.gemplotting.mapping.chamberInfo import chamber_config
#from system_specific_constants import chamber_config
#in chamber_config dict ohKey[0]=shelf, ohKey[1]=slot, ohKey[2]=optoHybrid
chambers = {}
for ohKey,cName in chamber_config.iteritems():
   if args.shelf == ohKey[0] and args.slot == ohKey[1]:
      chambers[ohKey[2]] = cName
   pass
pass

a_list = []
b_list = []
sc_list = []
name_list = []

#location of QC5 files
envCheck('DATA_PATH')
filePath  = os.getenv('DATA_PATH') + "/gainmap/"

#Loop on chambers in configuration file
for oh in chambers:
   print "oh ", oh
   detName = chambers[oh].replace('PAK', 'PAKISTAN')
   try:
      file = r.TFile(filePath+'GainMap_'+detName+'.root','read')
   except:
      print "Cannot open the file"

   #opening root TGraph from file
   file.cd('Summary/')
   gainCurve = file.Get('/Summary/g_'+detName+'_EffGainAvg')
   #Fitting the curve with exponential
   f = r.TF1( 'f', 'exp([1]*x + [0])', 0, 1000 )
   gainCurve.Fit( 'f' )
   #taking fit parameters
   a_list.append(f.GetParameter(0))
   b_list.append(f.GetParameter(1))
   file.Close()
   #Taking other infos for the dataframe: super-chamber (sc) index and detector name
   if oh % 2 == 0:
       sc_list.append(oh/2)
   else:
       sc_list.append((oh-1)/2)

   name_list.append(chambers[oh])

df = pd.DataFrame({'chamber': name_list, 'sc': sc_list, 'p0': a_list, 'p1': b_list})
del a_list, b_list, sc_list, name_list
df['gain'] = args.gain
df['I0'] = (math.log(args.gain)-df.p0 )/df.p1 * (P/964.4) * (297.1/(T))

#reversing dataframe to have same visualization as original configuration file
df = df.iloc[::-1].reset_index(drop=True)

#averaging I0 based on the "sc" (super-chamber) index because they have common power supply!
df['Iavg'] = df.groupby('sc')['I0'].transform('mean')

#for each sc, computing the gain spread at Iavg with respect to the desired gain
df['gainDiff'] =(np.exp(df.p1 * df.Iavg * (964.4/P) * (T)/297.1 + df.p0) - args.gain)
print(df)
