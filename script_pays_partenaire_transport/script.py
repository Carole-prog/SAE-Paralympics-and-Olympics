#On va essayer de faire un script qui marche :)

import csv 
import pymysql

conn = pymysql.connect(
    host="ssh.nathaan.com",
    user="user",
    password="pwd",
    database="olympics",
    port=3306
)

cursor = conn.cursor()

cursor.execute("TRUNCATE pays")
query = "INSERT INTO pays (ISO, nom_pays) VALUES (%s, %s)"
with open('Pays.csv', newline='') as csvfile:
    reader = csv.reader(csvfile,delimiter = ',')
    for ligne in reader :
        iso = ligne[0]
        nom = ligne[1]
        cursor.execute(query, (iso, nom))

cursor.execute("TRUNCATE partenaire")
query2 = "INSERT INTO partenaire(Nom_partenaire,Type_partenaire) VALUES (%s, %s)"
with open('Partenaire.csv',newline='')as partfile : 
    read = csv.reader(partfile,delimiter=',')
    for line in read:
        nomPart = line[1]
        typePart = line[0]
        cursor.execute( query2,(nomPart,typePart))

cursor.execute("TRUNCATE Transport")
query3 = "INSERT INTO Transport(Nom_transport,n°Lignes,Nbr_arrets,Lieux_dep,Terminus) VALUES (%s, %s , %s , %s , %s)"
with open('Type_Transport.csv',newline='')as vroumfile:
    reader = csv.reader(vroumfile,delimiter=',')
    for row in reader :
        nom = row[0]
        num = row[1]
        arret = None
        if row[2] != '':
            arret = row[2]
        dep = row[3]
        term = row[4]
        cursor.execute(query3, (nom, num, arret, dep, term))

conn.commit()
conn.close()
