#!/usr/bin/env python
from couchdbkit import Server, Database
import sys
  
theServer = sys.argv[1]
theDbName = sys.argv[2]
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
  
s = Server(theServer)
db = s[theDbName]

vr = db.all_docs()

docs = []
choice = ''
print ''
print 'For each record in the database, select whether it retains to a part (p) or'
print 'a material (m). If you don\'t know, select \'u\'. To stop, type \'s\'. When you stop,'
print 'all choices that you previous made will be uploaded to the database. These will be saved'
print 'so that the next time you run this script, you will not be asked again.'
print 'If you want to start over from the beginning, pass the word "all" when running this script'
print 'That is, run $> python materialOrPart.py all'
print ''
print 'To quit without changing the database, type \'q\''  

reviewAll = False
if len(sys.argv) > 1:
  if sys.argv[1] == 'all':
    reviewAll = True
  else:
    print sys.argv[1], 'is an invalid option.'
    sys.exit(0)
    
ignoreList = ['Nuclei', 'doctype', '_rev']
for row in vr:
  if choice == 's':
    break
  doc = db[row['id']]
  if doc.has_key('reviewed') == False or reviewAll:
    print ''
    for k,v in doc.iteritems():
      if k not in ignoreList:
        print '%s:  %s' % (k, v)
    choice = ''    
    while(choice != 'p' and choice != 'm' ):
      choice = raw_input('Part or Material (p/m/u/s/q):  ')
      if choice == 's' or choice == 'u':
        break
      if choice == 'q':
        print 'quiting. no changes were made to the database'
        sys.exit(0)
        
      if choice == 'p':
        doc['Part'] = doc['Material']
        doc['doctype'] = 'part_doc'
        del doc['Material']
        doc['reviewed'] = True
        docs.append(doc)
      if choice == 'm':
        doc['reviewed'] = True
        docs.append(doc)
  
  
print db.bulk_save(docs)
 