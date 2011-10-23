#!/usr/bin/env python
from couchdbkit import Server,Database
import sys, json, string, math, datetime
      
global yourName

#_______________
def formatvalue(value):
  if (isinstance(value, str)):  #SUPPORT UNICODE
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
    
#
#______________
def getDateDoc():
  dd = datetime.datetime.utcnow()
  datedoc =  {'year':dd.year, 'month':dd.month, 'day':dd.day, 'hour':dd.hour, 'minute':dd.minute, 'second':dd.second} 
  return datedoc
    
#______________
def parseDoc(doc):
    for k,v in doc.items():  
        doc[k] = formatvalue(v)
                
    return doc
#
#______________
def updateDocEntry(doc, name):
  dd = datetime.datetime.utcnow()
  datedoc =  {'year':dd.year, 'month':dd.month, 'day':dd.day, 'hour':dd.hour, 'minute':dd.minute, 'second':dd.second, 'microsecond':dd.microsecond} 
  if doc.has_key('doc_entries') == False:
    doc['doc_entries'] = []
  entry = {}
  entry['author'] = name
  entry['date_filed'] = datedoc
  doc['doc_entries'].append(entry)
  return doc
      
def generateBlankMeasureDoc(): 
  
  global yourName
  
  doc = {}
  doc['Activity'] = {}
  doc['Activity']['Limit'] = ''
  doc['Activity']['Uncertainty'] = ''
  doc['Activity']['Units'] = ''
  doc['Activity']['Value'] = ''
  doc['Activity']['Date_Measured'] = ''
  doc['Activity']['Date_Entered'] = ''
  doc['Detector'] = ''
  doc['Method'] = ''
  doc['Author'] = {}
  doc['Comments'] = ''
  doc['Entered By'] = yourName
  return doc
  
def addMeasurement(doc):
  
  #ask for the particular nuclei and then check the doc
  measurement = generateBlankMeasureDoc()
  nuclei = ''
  while nuclei == '':
    nuclei = raw_input('\nWhat is the name of the nuclei?  ')
                                      
  go = ''
  #
  while (go != 'y' and go != 'n'):
    go = raw_input('\nIs this a limit (y/n/c)? default=n:  ')
    if go == 'c':
      return doc
    if go == '':
      go = 'n'
      measurement['Activity']['Limit'] = False
    if go == 'y':
      measurement['Activity']['Limit'] = True
      
  for k in measurement.iterkeys():
    if k == 'Author':
      author = measurement[k]
      author['Measured By'] = []
      by = ''
      while (by != 'done'):
        by = raw_input('\nAdd an Author (leave blank when done):  ')
        if by == '': 
          by = 'done'
        else:
          author['Measured By'].append(by)
      
      author['Collaboration'] = raw_input('\nCollaboration:  ')
      measurement[k] =  author
    
    elif k == 'Activity':
      for act_key in measurement[k].iterkeys():
        if act_key != 'Limit': 
          
          if act_key == 'Date_Entered':
            measurement[k][act_key] = getDateDoc()

          elif act_key == 'Date_Measured':
            measurement[k][act_key] = {}        
            print '\nEnter the date this was measured. Use numbers for each field.'
            dateFields = ['Year', 'Month', 'Day']
            for df in dateFields:
              value = ''
              while (isinstance(value, int)) == False:
                value = formatvalue( raw_input('%s:  ' % df) )
                if isinstance(value, int):
                  measurement[k][act_key][df] = value
                elif value == '':
                  value = 0    
                  
          else:
            value = formatvalue( raw_input('\nEnter value for the field "' + act_key + '":  ') )
            measurement[k][act_key] = value
       
    elif k == 'Entered By':
      pass
    
    else: 
      value = formatvalue( raw_input('\nEnter value for the field "' + k + '":  ') )
      measurement[k] = value
  
  for k in measurement.keys(): 
    if measurement[k] == '':
      del measurement[k]    
      
  #  add more fields
  go = ''
  while (go != 'y' and go != 'n'):
    go = raw_input('\nDo you wish to add another field to the measurement? (y/n/c. default=n):  ') 
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'n'
    if go == 'y':
      key = raw_input('\nEnter a field name:  ')
      measurement[key] = formatvalue( raw_input('Enter value for the field "' + key + '":  ') )
      go = '' 
      
  #
  # review the document
  go = ''
  while (go != 'y' and go != 'n'):
    go = raw_input('\nDo you wish to review the measurement record? (y/n/c. default=n):  ')
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'n'
    if go == 'y':
      print json.dumps(measurement, indent = 1)
  
  #
  go = ''
  go = raw_input('\nIs this measurement record correct(y/n/c)? default=y:  ')
  if go == '':
    go = 'y'
  if go == 'y':  
                         
    nucDoc = {}
    thisNuc = {}
    thisNucMeasurements = []
    
    if doc.has_key('Nuclei') == False:
      doc['Nuclei'] = nucDoc
    else:
      nucDoc = doc['Nuclei']
                         
    if nucDoc.has_key(nuclei) == False:
      nucDoc[nuclei] = thisNuc
    else:
      thisNuc = nucDoc[nuclei]
      
    if thisNuc.has_key('Measurements') == False:
      thisNuc['Measurements'] = thisNucMeasurements
    else:
      thisNucMeasurement = thisNuc['Measurements']                    
      
    thisNuc['Measurements'].append(measurement) 
  
  return doc
           
def generateBlankDoc():
  doc = {}
  doc['Material'] = ''
  doc['Part'] = ''
  doc['Type'] =  ''
  doc['Reference'] =  ''
  doc['Provider'] = ''
  doc['reviewed'] = True
  
  return doc
     
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
    
def main():
   
  server = raw_input('Enter the server (default = edwdbik.fzk.de:6984):  ')
  if server == '':
    server = 'edwdbik.fzk.de:6984'
  
  protocol = 'https'
  port = server.split(':')
  if len(port) > 1:
    if port[1] != '6984':
      protocol = 'http'
  else:
    protocol = 'http'
    
  username = raw_input('Enter the username:  ')
  password = raw_input('Enter the password:  ')
  dbname = raw_input('Enter the database name (leave blank for default:materials):  ')
  if dbname == '':
    dbname = 'materials'
  if username != '':
    s = Server('%s://%s:%s@%s' % (protocol, username, password, server))
  else:
    s = Server('%s://%s' % (protocol, server))
  db = s[dbname]
               
  try:
    print 'Connection: ', db['test_connection']['result']
  except:
    print 'Can\'t connect to the database. You\'ve entered some incorrect information or your netword connection is down.'
    print 'try again'
    sys.exit(0)
  
  global yourName                                                  
  yourName = ''
  while yourName == '':
    yourName = raw_input('Enter your name:  ')
      
  go = ''
  print '\nAnswer the following questions and enter the requested data. You may cancel this script at any time'
  print 'by entering the letter "c" when asked a yes or no question.\n'
  print 'The data you enter here will be uploaded into a document on the database.\n'
    
  
  
  #start a new document and get the input values for each standard key  
  doc = generateBlankDoc()
  for k in doc.iterkeys():
    if k != 'reviewed': 
      value = formatvalue( raw_input('Enter value for the field "' + k + '":  ') )
      doc[k] = value
       
 
  
  # set standard docid and then check the database to see 
  # if this already exists. 
  docid = getDocId(doc)
  
  while (go != 'y' and go != 'n'):
    print '\nThe standard _id, a concatenation of Material, Type, Provider and Reference, is %s' % docid
    go = raw_input('Do you wish to use a different _id? (y/n/c.  default=n):  ')
    if go == 'c':
      sys.exit(0)     
    if go == '':
      go = 'n'
    if go == 'y':
      newid = raw_input('Enter a new _id:  ')
      if newid.startswith('_design'):
        print docid, 'is unacceptable. try again'
        go = ''
      else:
        docid = newid
  
  # check for the document on the database
  if db.doc_exist(docid):                    
    go = ''
    print '\nThis document already exists on the database'
    while (go != 'y' and go != 'n'):
      go = raw_input('Do you wish to append information to this document (y/n/c)? If you answer \'n\', this program will exit. default=y:  ')
      if go == 'c':
        sys.exit(0)
      if go == '':
        go = 'y'
      elif go == 'n':
        sys.exit(0)
    
    doc = db[docid]
        
  else:
    # select the doc type - part_doc or material_doc   
    go = ''
    while (go != 'p' and go != 'm'):
      go = raw_input('\nIs this a part (p) or a material (m) (or cancel=c)?  ')
      if go == 'c':
        sys.exit(0)
    
    if go == 'p':
      doc['doctype'] = 'part_doc'
      if doc['Material'] == '':
        del doc['Material']
    else:
      doc['doctype'] = 'material_doc'
      if doc['Part'] == '':
        del doc['Part']                             
  #
  # review the document
  go = ''
  while (go != 'y' and go != 'n'):
    go = raw_input('\nDo you wish to review the document? (y/n/c. default=n):  ')
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'n'
    if go == 'y':
      print json.dumps(doc, indent = 1)
      
  # add any new fields
  go = ''
  while (go != 'y' and go != 'n'):
    go = raw_input('\nDo you wish to add another field to the document? (y/n/c. default=n):  ') 
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'n'
    if go == 'y':
      key = raw_input('Enter a field name:  ')
      doc[key] = formatvalue( raw_input('Enter value for the field "' + key + '":  ') )
      go = ''
        
  
  # add measurements
  go = ''
  while (go != 'n'):
    go = raw_input('\nAdd Measurement? (y/n/c. default=y):  ')
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'y'
    if go == 'y':
      doc = addMeasurement(doc)
  
  
  # review the document
  go = ''
  while (go != 'y' and go != 'n'):
    go = raw_input('\nDo you wish to review the document? (y/n/c. default=y):  ')
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'y'
    if go == 'y':
      print json.dumps(doc, indent = 1)
                    
  
  # save to the database
  go = ''   
  while (go != 'y' and go != 'n'):
    go = raw_input('\nDo you wish to upload the document to the database? (y/n/c. default=y):  ')
    if go == 'c':
      sys.exit(0)
    if go == '':
      go = 'y'
    if go == 'y':
      print 'savng to database. the following is the database response'
      doc = updateDocEntry(doc, yourName)
      print db.save_doc(doc)

if __name__ == '__main__':
  main()
  