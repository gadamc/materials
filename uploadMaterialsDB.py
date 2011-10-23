#!/usr/bin/env python
# -*- coding: utf-8 -*-

from couchdbkit import Server, Database
from couchdbkit.loaders import FileSystemDocsLoader
import time, sys, subprocess, math, os, datetime, string, copy

#----------
# taken from the Python website in order to handle unicode, which is need in order to properly
# deal with different kinds of characters
import csv, codecs, cStringIO
      
# #connect to the db
global theServer
global db 
 
def UnicodeDictReader(str_data, encoding, **kwargs):
    csv_reader = csv.DictReader(str_data, **kwargs)
    # Decode the keys once
    keymap = dict((k, k.decode(encoding)) for k in csv_reader.fieldnames)
    
    #for row in csv_reader:
    #  for k, v in row.iteritems():
    #    print str_data.name,'key',k,'value',v
    
    for row in csv_reader:
        yield dict((keymap[k], v.decode(encoding)) for k, v in row.iteritems())
  
#_______________
def formatvalue(value):
  if (isinstance(value, basestring)):  #SUPPORT UNICODE
    # #see if this string is really an int or a float
    if value.isdigit()==True: #int
      return int(value)
    else: #try a float
      try:
        if math.isnan(float(value))==False:
          return float(value)
      except:
        return value

    return value.strip('" ') #strip off any quotations and extra spaces found in the value
  else:
    return value
    
  
#______________
def parseDoc(doc):
    for k,v in doc.items():  
        doc[k] = formatvalue(v)
                
    return doc

#______________
def parseLineToArray(line, token):
  ml = line.split(token)
  for i in range(len(ml)):
    ml[i] =  ml[i].strip() #strip off any extra spaces
     
  return ml

#______________
def parseKeyIntoArray(doc, key, token):
  if doc.has_key(key):
    doc[key] = parseLineToArray(doc[key], token)

#______________
def getDateDoc():
  dd = datetime.datetime.utcnow()
  datedoc =  {'year':dd.year, 'month':dd.month, 'day':dd.day, 'hour':dd.hour, 'minute':dd.minute, 'second':dd.second} 
  return datedoc
  
#______________
def getDocId(doc):
  
  anId = doc['Material']     
  fields = ['Type', 'Provider', 'Reference'] 
  for field in fields:
    if doc.has_key(field):
      if doc[field] != '':
        anId += '_%s' % doc[field]
  
  anId = string.replace(anId, ' ', '_')  
  return anId
#______________
def getConcentrationsActivity(gamma, aNuc): 
  
  activity = {} 
  if gamma[aNuc] != 'NULL' and gamma[aNuc] != '':
    activity['Value'] = formatvalue(gamma[aNuc])
  else:
    return -1
      
  print '   activity', activity['Value']     

  activity['Limit'] = True if gamma['limit'+aNuc]=='<' else False 
  
  if gamma['error'+aNuc] != 'NULL':
    activity['Uncertainty'] = formatvalue(gamma['error'+aNuc])
  else:
    activity['Uncertainty'] = ''
    
  activity['Units'] = gamma['Units']
  
  
  return activity
  
#______________
def uploadConcentrations(afile):
   
  global db  
  global authorName
       
  reader = UnicodeDictReader(afile, 'utf-8', dialect = 'excel')   
  for rawdoc in reader:
    doc = parseDoc(rawdoc)
    if doc['Material'] == '':
      continue
       
    docid = getDocId(doc)   
    gamma = copy.deepcopy(doc)
    nucList = ['234Th', '214Pb', '214Bi', '228Ac', '212Pb','208Tl', '235U', '210Pb','137Cs','40K','60Co']
    nucDelList = []
    for nuc in nucList:
      nucDelList.append(nuc)
      nucDelList.append('limit'+nuc)
      nucDelList.append('error'+nuc)
    delList = nucDelList + ['Collaboration', 'Detector', 'Measured_by', 'Units']
    for item in delList:
      del doc[item]
    
    nuclei = {}
    if db.doc_exist(docid):
      doc = db[docid]
      nuclei = copy.deepcopy(doc['Nuclei'])    
    else:                
      doc['_id'] = docid
                                
    
    for nuc in nucList: 
      print 'concentration', docid, nuc
      data = {}
      if gamma[nuc] == '':
        continue
      
      act = getConcentrationsActivity(gamma, nuc)
      if type(act) == type({}):
        data['Activity'] = act  
      data['Detector'] = gamma['Detector'] 
      data['Author'] = getGammaMeasured(gamma)  
      doc['Nuclei'] = appendData(nuclei, data, nuc)
    
    doc['doctype'] =  'material_doc'
    db.save_doc(doc)
    
#______________
def appendData(nuclei, data, aNuc):
  measurements = []
  data['Date_Entered'] = getDateDoc()
  
  if nuclei.has_key(aNuc):
    if nuclei[aNuc].has_key('Measurements'):
      measurements = copy.deepcopy(nuclei[aNuc]['Measurements'])
  else:
    nuclei[aNuc] = {}
      
  measurements.append(data) 
  nuclei[aNuc]['Measurements'] = measurements         
  
  return nuclei

#______________
def getGammaActivity(gamma, aNuc): 
  
  activity = {}
  activity['Value'] = gamma[aNuc].strip(' ') if type(gamma[aNuc]) == type('') else str(gamma[aNuc])    #remove leading and trailing spaces  
  print '   activity', activity['Value']
  
  if activity['Value'] == '' or activity['Value'].startswith('-'):
    return -1
    
  activity['Limit'] = True if activity['Value'].startswith('<') else False
  if activity['Limit'] == False:
    if activity['Value'].find('(') > -1:
      activity['Uncertainty'] = formatvalue( activity['Value'].split('(')[1].split(')')[0].strip(' ') )   
      activity['Value'] = formatvalue( activity['Value'].split('(')[0].strip(' ') )
    else:
      activity['Uncertainty'] = ''
      activity['Value'] = formatvalue( activity['Value'] )
  else:
    activity['Uncertainty'] = ''
    activity['Value'] = formatvalue( activity['Value'].split('<')[1].strip(' ') )
  
  if gamma.has_key('Units'):
    activity['Units'] = gamma['Units']
  
  
  return activity      
#______________
def getGammaMeasured(gamma):
  measured = {}
  authors = gamma['Measured_by'].split('&')
  for i in range(len(authors)):
    authors[i] = authors[i].strip(' ')
  measured['Measured By'] = authors
  if gamma.has_key('Collaboration'):
    measured['Collaboration'] = gamma['Collaboration']
  
  return measured
  
#______________
def uploadGamma(afile):
   
  global db  
  global authorName
       
  reader = UnicodeDictReader(afile, 'utf-8', dialect = 'excel')   
  for rawdoc in reader:
    doc = parseDoc(rawdoc)
    if doc['Material'] == '':
      continue
       
    docid = getDocId(doc)   
    gamma = copy.deepcopy(doc)
    nucList = ['234Th', '214Pb', '214Bi', '228Ac', '212Pb','208Tl', '235U', '210Pb','137Cs','40K','60Co']
    delList = nucList + ['Collaboration', 'Detector', 'Comments', 'Measured_by', 'Units']
    for item in delList:
      del doc[item]
    
    nuclei = {}
    if db.doc_exist(docid):
      doc = db[docid]
      nuclei = copy.deepcopy(doc['Nuclei'])    
    else:                
      doc['_id'] = docid
                                
    
    for nuc in nucList: 
      print 'gamma', docid, nuc
      data = {}
      if gamma[nuc] == '':
        continue
        
      act = getGammaActivity(gamma, nuc)
      if type(act) == type({}):
        data['Activity'] = act
        data['Comments'] = gamma['Comments']
        data['Detector'] = gamma['Detector'] 
        data['Author'] = getGammaMeasured(gamma)
        data['Entered By'] = 'ILIAS radiopurity.in2p3.fr. Gamma' 
        doc['Nuclei'] = appendData(nuclei, data, nuc)
      
    doc['doctype'] =  'material_doc'
    db.save_doc(doc)
           
#______________
def uploadUkdm(afile):
   
  global db  
  global authorName
       
  reader = UnicodeDictReader(afile, 'utf-8', dialect = 'excel')   
  for rawdoc in reader:
    doc = parseDoc(rawdoc)
    if doc['Material'] == '':
      continue
       
    docid = getDocId(doc)
    print docid   
    gamma = copy.deepcopy(doc)
    nucList = ['238U', '232Th','40K']
    delList = nucList + ['Method', 'comments', 'Measured_by']
    for item in delList:
      del doc[item]
    
    nuclei = {}
    if db.doc_exist(docid):
      doc = db[docid]
      nuclei = copy.deepcopy(doc['Nuclei'])    
    else:                
      doc['_id'] = docid
                                
    
    for nuc in nucList: 
      print 'ukdm', docid, nuc
      data = {}
      if gamma[nuc] == '':
        continue
        
      act = getGammaActivity(gamma, nuc)
      if type(act) == type({}):
        data['Activity'] = act
        data['Comments'] = gamma['comments']
        data['Method'] = gamma['Method'] 
        data['Author'] = getGammaMeasured(gamma)
        data['Entered By'] = 'ILIAS radiopurity.in2p3.fr. UKDMdata'  
        doc['Nuclei'] = appendData(nuclei, data, nuc)
    
    doc['doctype'] =  'material_doc'
    db.save_doc(doc)


#______________
def uploadRadon(afile):
   
  global db  
  global authorName
       
  reader = UnicodeDictReader(afile, 'utf-8', dialect = 'excel')   
  for rawdoc in reader:
    doc = parseDoc(rawdoc)
    if doc['Material'] == '':
       continue
       
    docid = getDocId(doc)   
    
    data = copy.deepcopy(doc)
    del data['Material']
    del doc['Activity']
    del doc['Surface'] 
    del doc['Ref']
     
    nuclei = {}
    if db.doc_exist(docid):
      doc = db[docid]
      nuclei = copy.deepcopy(doc['Nuclei'])    
    else:                
      doc['_id'] = docid
    
    print 'radon', docid
    
    data['Entered By'] = 'ILIAS radiopurity.in2p3.fr. Radon'
    doc['Nuclei'] = appendData(nuclei, data, 'Rn')    
    doc['doctype'] =  'material_doc'
    db.save_doc(doc)

#______________
def uploadData(fname, uri, dbname):
     
  print 'Upload contents of %s to %s/%s' % (fname, uri, dbname)
  
  
  gamma = open(os.path.join(dir, 'Gamma.csv'), 'r')   
  concentrations =  open(os.path.join(dir, 'Concentrations.csv'), 'r')   
  ukdm = open(os.path.join(dir, 'UKDMdata.csv'), 'r')
  radon =  open(os.path.join(dir, 'Radon.csv'), 'r') 
   
  uploadRadon(radon)
  uploadGamma(gamma)
  #uploadConcentrations(concentrations)
  uploadUkdm(ukdm)
  
  global db
  test = {}
  test['_id'] = 'test_connection'
  test['result'] = 'success'
  db.save_doc(test)
  #used for bulk uploading
  

  
#______________
if __name__=='__main__':
  dir = sys.argv[1]
  uri = sys.argv[2]
  dbname = 'materials' 
  # #connect to the db
  global theServer
  global db 
  # #connect to the db
  theServer = Server(uri)
  db = theServer[dbname]
  print uri, dbname
   
  if len(sys.argv) >= 4:
    dbname = sys.argv[3]
  
  uploadData(dir, uri, dbname)
  


