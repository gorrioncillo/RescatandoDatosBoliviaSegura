import urllib.request, json, pprint, os.path, csv, time, sqlite3, tinydb
from datetime import datetime

urlBolSeg = 'https://boliviasegura.agetic.gob.bo/wp-content/json/api.php'
bolSegCsv = 'bolSegDatos.csv'
prevJsonData = 'oldBolSegCov.json'
timesToCheck = 5
bolSegSqlite = 'bolSegDatos.s3db'
bolSegSqliteTable = 'bolCovid19'
bolSegTinydb = 'bolSegDatos.json'
bolSegTinydbTable = 'bolCovid19'

def getDataJson():
  datos = urllib.request.urlopen(urlBolSeg)
  #datos = open('bolSegJson-02-05-2020.json','r')
  jsonDatos = json.load(datos)
  return jsonDatos

def checkOldDatetimeJsonFile(newJsonData):
  if (os.path.exists(prevJsonData)):
    oldDatos = open(prevJsonData)
    oldJsonData = json.load(oldDatos)
    oldDatetime = datetime.strptime(oldJsonData['fecha'],\
                                    '%d/%m/%y %H:%M')
    newDatetime = datetime.strptime(newJsonData['fecha'],\
                                    '%d/%m/%y %H:%M')
    if (oldDatetime>=newDatetime):
      return False
  return True

def newToOldJson(newjsonData):
  with open(prevJsonData,'w', encoding= 'utf-8') as oldFile:
    json.dump(newjsonData, oldFile, ensure_ascii=False)

def flattenJson(objJson, delim):
  val = {}
  for i in objJson.keys():
    if isinstance( objJson[i], dict ):
      get = flattenJson( objJson[i], delim )
      for j in get.keys():
        val[i + delim + j] = get[j]
    else:
      val[i] = objJson[i]
  return val

def putDataCsv(outFile, objDic):
  columns = [x for x in objDic.keys()]
  columns = list(set( columns ))
  columns.sort()
  if not(os.path.exists(outFile)):
    with open(outFile, 'a', newline='') as outputFile:
      csvW = csv.DictWriter(outputFile, fieldnames=columns)
      csvW.writeheader()
  with open(outFile, 'a', newline='') as outputFile:
    csvW = csv.DictWriter(outputFile, fieldnames=columns)
    csvW.writerow(objDic)
  return columns

def putDataSqlite(database, objDic, table='bolCovid19'):
  query = f'INSERT INTO {table} VALUES (?)'
  conn = sqlite3.connect(database)
  c = conn.cursor()
  c.execute(query, [json.dumps(objDic)])
  conn.commit()
  conn.close()

def getDataSqlite(database, table='bolCovid19'):
  query = f'SELECT * FROM {table}' #bolCovid19
  conn = sqlite3.connect(database)
  c = conn.cursor()
  c.execute(query)
  rows = c.fetchall()
  for row in rows:
    m = json.loads(row[0])
    print(m['fecha'], m['contador']['confirmados'], m['contador']['decesos'])
  conn.close()

def putDataTinydb(database, objDic, table='bolCovid19'):
  db = tinydb.TinyDB(database)
  table = db.table(bolSegTinydbTable)
  table.insert(objDic)
  db.close()

def getDataTinydb(database, table='bolCovid19'):
  db = tinydb.TinyDB(database)
  table = db.table(bolSegTinydbTable)
  #query = tinydb.Query()
  for r in table:
    print(r['fecha'], r['contador']['confirmados'], r['contador']['decesos'])
  db.close()


def main(): 
  pass

if __name__ == '__main__':
  #main()
  do = True
  counter = 0
  while(do and (counter<timesToCheck)):
    jsonData = getDataJson()
    #pprint.pprint(jsonDatos)
    isNewJson = checkOldDatetimeJsonFile(jsonData)
    print(isNewJson)
    if(isNewJson):
      newToOldJson(jsonData)
      flatDic = flattenJson(jsonData,'.')
      #pprint.pprint(flatDic)
      columnas = putDataCsv(bolSegCsv, flatDic)
      #pprint.pprint(columnas)
      putDataSqlite(bolSegSqlite, jsonData, bolSegSqliteTable)
      putDataTinydb(bolSegTinydb, jsonData, bolSegTinydbTable)
      do = False
    else:
      time.sleep(1)#30*60)
      counter += 1
  
  if (counter==timesToCheck):
    print(f'Se intento {timesToCheck} veces')
    print('No se encontraron nuevos datos')

  getDataSqlite(bolSegSqlite, bolSegSqliteTable)
  print()
  getDataTinydb(bolSegTinydb, bolSegTinydbTable)