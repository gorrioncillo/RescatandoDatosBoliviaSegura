import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import matplotlib
matplotlib.use('Qt5Agg')
import urllib.request, urllib.error,\
  json, pprint, os.path, csv, time, sqlite3, tinydb,\
  matplotlib.pyplot as plt, matplotlib.dates as dates
from datetime import datetime

urlBolSeg = 'https://boliviasegura.agetic.gob.bo/wp-content/json/api.php'
bolSegCsv = 'bolSegDatos.csv'
prevJsonData = 'oldBolSegCov.json'
timesToCheck = 1
timeSleep = 1 #30*60 #en segundos
bolSegSqlite = 'bolSegDatos.s3db'
bolSegSqliteTable = 'bolCovid19'
bolSegTinydb = 'bolSegDatos.json'
bolSegTinydbTable = 'bolCovid19'

def getDataJson():
  try:
    datos = urllib.request.urlopen(urlBolSeg)
    #datos = open('bolSegJson-02-05-2020.json','r')
    jsonDatos = json.load(datos)
  except urllib.error.URLError as e:
    if hasattr(e, 'reason'):
      print('No pudimos conectarnos al servidor')
      print("Revize su conexion a internet")
      print('Razon: ', e.reason)
      return None
    elif hasattr(e, 'code'):
      print('El servidor no pudo manejar el pedido')
      print('Error code: ', e.code)
      return -1
  except json.JSONDecoderError as e:
    print('Hubo un error al leer el recurso JSON')
    return -1
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
    json.dump({'fecha':newjsonData['fecha']}, oldFile, ensure_ascii=False)
    #json.dump(newjsonData, oldFile, ensure_ascii=False)

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
  bolDatetime = []
  bolCases = []
  bolDeceases = []

  cbbaCases = []
  cbbaDeceases = []

  query = f'SELECT * FROM {table}' #bolCovid19
# si la tabla no existe crearla, campos aplanados
  conn = sqlite3.connect(database)
  c = conn.cursor()
  c.execute(query)
  rows = c.fetchall()

  for row in rows:
    m = json.loads(row[0])
    bolDatetime.append(datetime.strptime(m['fecha'].split(' ')[0], '%d/%m/%y'))
    bolCases.append(m['contador']['confirmados'])
    bolDeceases.append(m['contador']['decesos'])
    cbbaCases.append(m['departamento']['cb']['contador']['confirmados'])
    cbbaDeceases.append(m['departamento']['cb']['contador']['decesos'])

  conn.close()
  return bolDatetime, bolCases, bolDeceases, cbbaCases, cbbaDeceases

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

def newPeople(tupData):
  #q = tupData[0]
  a = [(tupData[1][n]-tupData[1][n-1]) for n in range(1,len(tupData[1]))]
  a.insert(0,0)
  b = [(tupData[2][n]-tupData[2][n-1]) for n in range(1,len(tupData[2]))]
  b.insert(0,0)
  c = [(tupData[3][n]-tupData[3][n-1]) for n in range(1,len(tupData[3]))]
  c.insert(0,0)
  d = [(tupData[4][n]-tupData[4][n-1]) for n in range(1,len(tupData[4]))]
  d.insert(0,0)
  return a,b,c,d

def ploting(n, x, y ,z, w, v):
  x = dates.date2num(x)
  hfmt = dates.DateFormatter('%m\n%d')

  fig = plt.figure(n)
  axs = fig.subplots(nrows=2, ncols=1)

  axs[0].bar(x, w, width=0.15, alpha=0.5, color='yellow',
            label='Nuevos Casos')
  axs01 = axs[0].twinx()
  axs01.scatter(x, y, color='blue', label='Casos Acumulados')
  #ax = fig.gca()
  axs[0].xaxis.set_major_locator(dates.DayLocator())
  axs[0].xaxis.set_major_formatter(hfmt)


  axs[1].bar(x, v, width=0.15, alpha=0.5, color='black',
            label='Nuevas Muertes')
  axs11 = axs[1].twinx()
  axs11.scatter(x, z, color='red', label='Muertes Acumuladas')

  axs[1].xaxis.set_major_locator(dates.DayLocator())
  axs[1].xaxis.set_major_formatter(hfmt)

  #Formateo la caja de informacion de coordenadas
  axs01.format_xdata = dates.DateFormatter('%d-%m-%Y')
  axs01.format_ydata = lambda x: '%1.0f Casos' % x
  axs01.grid(True)
  axs11.format_xdata = dates.DateFormatter('%d-%m-%Y')
  axs11.format_ydata = lambda x: '%1.0f Casos' % x
  axs11.grid(True)
  fig.autofmt_xdate()
  fig.subplots_adjust(top=0.88, bottom=0.1, left=0.05, right=0.94,
                      hspace=0.1, wspace=0.2)
  fig.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5,0.955))
  return fig

def printTable(hed, *args):
  formatStr = "{:<11}" + ("{:<9}" * (len(hed)-1))
  print(formatStr.format(*hed))
  for i,v in enumerate(args[0]):
    rec = [it[i] for it in args]
    print(formatStr.format(*rec))


def main():
  pass

if __name__ == '__main__':
  print("Comenzando el programa\n")
  #main()
  do = True
  counter = 0
  while(do and (counter<timesToCheck)):
    jsonData = getDataJson()
    if not isinstance(jsonData, dict):
      break
    elif isinstance(jsonData, int):
      counter +=1
      print("Intentando nuevamente")
      continue

    isNewJson = checkOldDatetimeJsonFile(jsonData)

    if(isNewJson):
      newToOldJson(jsonData)
      flatDic = flattenJson(jsonData,'.')
      columnas = putDataCsv(bolSegCsv, flatDic)
      putDataSqlite(bolSegSqlite, jsonData, bolSegSqliteTable)
      putDataTinydb(bolSegTinydb, jsonData, bolSegTinydbTable)
      do = False
    else:
      print(f"No hay nuevos datos.\nIntentando en {timeSleep} segs")
      counter += 1
      time.sleep(timeSleep)

  if (counter==timesToCheck):
    print(f'\nSe intento {timesToCheck} veces')
    print('No se encontraron nuevos datos\n')

  aux1 = getDataSqlite(bolSegSqlite, bolSegSqliteTable)
  xDate, aCases, aDeaths, cbCases, cbDeaths = aux1
  xDateStr = [x.date().strftime('%d-%m-%y') for x in xDate]
  aux2 = newPeople(aux1)
  aNewCases, aNewDeaths,cbNewCases,cbNewDeaths = aux2

  print('{:<46} {:<46}'.format('','Nuevos'))
  headers=['Fecha', 'Casos', 'Muertes','cbCasos', 'cbMuet',
          'Casos', 'Muertes', 'cbCasos', 'cbMuet']
  aux2 = aux1 + aux2
  if(len(xDateStr)>=20):
    printTable(headers, xDateStr[-20:], *[a[-20:] for a in aux2[1:]])
  else:
    printTable(headers, xDateStr, *aux2[1:])

  #fig1 = ploting(1, xDate, aCases, aDeaths, aNewCases, aNewDeaths)
  #fig1.suptitle('Grafica Bolivia')
  #fig1.canvas.set_window_title('Graficas Bolivia')
  #fig2 = ploting(2, xDate, cbCases, cbDeaths, cbNewCases, cbNewDeaths)
  #fig2.suptitle('Grafica Cochabamba')
  #fig2.canvas.set_window_title('Graficas Cochabamba')
  #plt.show()
  print()
  #getDataTinydb(bolSegTinydb, bolSegTinydbTable)
