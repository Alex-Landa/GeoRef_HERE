import psycopg2
import requests
import time
class BaseDatos:
    def __init__(self):
        self.connection = psycopg2.connect(
                host = "",
                user = "",
                password = "",
                database = "",
                port=""
            )
        print("Conexión Exitosa")
    def updateOne(self,consulta):
        self.cursor = self.connection.cursor()
        self.sql = consulta
        self.cursor.execute(self.sql)
        self.connection.commit()
        self.cursor.close()
    def qwerysAll(self,consulta):
        self.cursor = self.connection.cursor()
        self.sql = consulta
        self.cursor.execute(self.sql)
        self.rows = self.cursor.fetchall()
        self.cursor.close()
        return self.rows
db=BaseDatos()

tables=[]#tables to modify
keys=["DdUCHFkxcVIVAQAFJYYntirnE10nfO2-I99VhRKokf0"]#here key or keys
db=BaseDatos()
fields=["ID","Colony","Adress","LAT","LON"]# fields of your DB

def Postal(ide):#format to postal code
  num=""
  ceros=""
  for x in range(5-len(format(ide))):
    ceros=ceros+"0"
  num=ceros+format(ide)
  return num


def coords(direccion,key):
    url3=f"https://geocode.search.hereapi.com/v1/geocode?q={direccion}&limit=4&apiKey={key}"
    print(direccion)
    response=requests.get(url3)
    print(response)
    try:
        if response.status_code==200:
            data=response.json()
            cartesiano=data.get("items")
            cartesiano=cartesiano[0].get("position")
            latitud=cartesiano.get("lat")
            longitud=cartesiano.get("lng")
            return [latitud,longitud],0
        elif response.status_code==429:
            return ["ERR","ERR"],1
        else:
            return ["ERR","ERR"],2
    except:
        return ["ERR","ERR"],2
  #-----------------------------------------------------------------------------  
def cleanAddress(Direction,Country="Mexico"):
    street=Direction.split(",")
    colony=street[3].strip(" ").split(" ")
    colony="+".join(colony)
    cp=Postal(street[2].strip(" "))
    col=street[1].strip(" ").split(" ")
    col="+".join(col)
    street=street[0].strip(" ").split(" ")
    ##########clear your input
    nDire=street+"+"+col+"+"+cp+"+"+colony.replace(" ","+")+"+"+Country
    return nDire

def geoRef(key):
    rev=[] 
    cont=0
    useKey=0###key used
    control=1
    while control<len(tables):
        badAdress=db.qwerysAll(F"select {fields[0]},{fields[1]},{fields[2]} FROM {tables[control]} where {fields[3]} =0 AND {fields[4]} =0 order by {fields[0]} desc")
        while len(badAdress) >0 :
            Adress=cleanAddress(badAdress[0][2])
            Morsa,err=coords(Adress,key[useKey])
            print(f"key: {badAdress[0][0]}")
            if err==0:
                print(str(Morsa[0])+" "+str(Morsa[1]) + " coords")
                db.updateOne(f"""update {tables[control]} set lat = {Morsa[0]} , lon = {Morsa[1]}  where ID='{str(badAdress[0][0])}' """ )    
                print(f"query ran on {badAdress[0][0]}")
                badAdress.pop(0)####pop the searched address
                cont=cont+1
                print(str(cont)+ " queries made")
                print("--------------------------------------------------------------------------------------")
            elif err==1:
                print("--------------------------------------------------------------------------------------")
                useKey+=1
                print(f"changing to key {useKey}...")
                time.sleep(1)
            else:
                rev.append(badAdress[0])
                badAdress.pop(0)
                print(rev)
            print("-------------------------------------------------------------------------------------------------------") 
        control+=1
        print(f"cambio de tabla a {tables[control]}")
        time.sleep(1)
            
geoRef(keys)        