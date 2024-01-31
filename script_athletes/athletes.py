import requests
import pymysql
import hashlib
import os
import re
import html
from datetime import date, datetime

def get(url):
    # get with caching
    
    filename = "cache/" + url + ".html"
    if os.path.isfile(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = f.read()
            
        return data
        
    r = requests.get("https://www.olympedia.org/" + url, allow_redirects=False)
    if r.status_code != 200:
        raise Exception("Status %d" % r.status_code)
        
    dirname = os.path.dirname(filename)
    os.makedirs(dirname, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(r.text)
        
    return r.text
    
def getAthleteBio(atheleteId):
    text = get("athletes/" + str(athleteId))
    
    # quick regex to find biodata
    match = re.search(r"<table class='biodata'>(.*?)</table>", text, re.DOTALL)
    assert match is not None
    biodata = match.group(1)
    
    data = {}
    for line in biodata.split("</tr>")[:-1]:
        line = line.strip()
        
        assert line.count("<tr>") == 1
        assert line.count("<th>") == line.count("</th>") == 1
        assert line.startswith("<tr><th>") 
        
        name, value = line[8:].split("</th><td>")
        name = name.strip()
        
        value = value.strip()
        assert value.endswith("</td>")
        value = value[:-5]
        
        data[name] = html.unescape(value)
    
    return data
    
    

conn = pymysql.connect(
    host="ssh.nathaan.com",
    user="user",
    password="pwd",
    database="olympics",
    port=3306,
    autocommit=True
)
cursor = conn.cursor()

# analyse
athKeys = set()
athKeysOptional = set()
athBioExamples = {}

cursor.execute("DELETE FROM medailles_athletes")
cursor.execute("DELETE FROM athletes")

cursor.execute("SELECT id, nom FROM sports")
    
for sportId, sportName in cursor.fetchall():
    if sportId == "BKG":
        continue
        
    elif sportId == "EQU":
        continue
        
    else:
        text = get("sports/" + sportId)
        
    match = re.search(r"<h1>([^<]+)</h1>", text)
    #print("|", sportId, "|", sportName.center(24), "|", match.group(1).center(24), "|")
    
    match = re.search(r"<h2>Most successful competitors</h2>\s*<h3>Olympic Games</h3>\s*<table[^>]+>\s*<thead>.*?</thead>(.*?)</table>", text, re.DOTALL)
    assert match is not None
    
    text = match.group(1)
    
    matches = re.findall(r"<tr class='top'>\s*<td><a href=\"/athletes/([0-9]+)\">([^<]+)</a></td>\s*<td><a href=\"/countries/([A-Z]+)\">.*?</td>\s*<td>([0-9]+)</td>\s*<td>([0-9]+)</td>\s*<td>([0-9]+)</td>\s*<td>([0-9]+)</td>", text)
    
    assert len(matches) == text.count("<tr class='top'>")
    for athleteId, athleteName, athleteCountry, goldMedals, silverMedals, bronzeMedals, totalMedals in matches:
        athleteId = int(athleteId)
        bio = getAthleteBio(int(athleteId))
        # on garde id athlete de olympedia pour "faciliter"

        # Analyse
        # Clés obligatoires : {'Full name', 'Used name', 'Roles', 'Sex', 'NOC'}
        # Clés possibles : {'Other names', 'Died', 'Name order', 'Measurements', 'Roles', 'Nick/petnames', 'Title(s)', 'Used name', 'NOC', 'Affiliations', 'Original name', 'Born', 'Nationality', 'Sex', 'Full name'}

        #athKeys |= bio.keys()
        #
        #for key, value in bio.items():
        #    if key not in athBioExamples:
        #        athBioExamples[key] = set()
        #        
        #    athBioExamples[key].add(value)
        #    
        #for key in athKeys:
        #    if key not in bio:
        #        athKeysOptional.add(key)
        
        # date de naissance et décès
        MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        
        born = None
        if "Born" in bio:
            match = re.search("^([0-9]+) (" + "|".join(MONTHS) + ") ([0-9]{4})", bio["Born"])
            assert match is not None
            
            born = date(int(match.group(3)), 1+MONTHS.index(match.group(2)), int(match.group(1)))
            
        died = None
        if "Died" in bio:
            match = re.search("^([0-9]+) (" + "|".join(MONTHS) + ") ([0-9]{4})", bio["Died"])
            if match is None:
                match = re.search("^([0-9]{4})", bio["Died"])
                assert match is not None
                
                died = None
            
            else:
                died = date(int(match.group(3)), 1+MONTHS.index(match.group(2)), int(match.group(1)))
                
        taille = None
        poids = None
        if "Measurements" in bio:
            match = re.match("([0-9]+) cm / ([0-9]+) kg", bio["Measurements"])
            if match:
                taille = int(match.group(1))
                poids = int(match.group(2))
                
            else:
                match = re.match(r"([0-9]+) kg", bio["Measurements"])
                if match:
                    poids = int(match.group(1))
                    
                else:
                    match = re.match(r"([0-9]+) cm", bio["Measurements"])
                    if match:
                        taille = int(match.group(1))
                        
                    else:
                        print("no match", bio["Measurements"])
                    
            
        assert bio["Sex"] in ("Male", "Female") # bon.
        sex = "M" if bio["Sex"] == "Male" else "F"
        
        print(bio["Full name"].center(50), "|", str(born).center(15), "|", str(died).center(15))
        cursor.execute(
            "INSERT INTO athletes (id, nom, genre, nationalite, taille, poids, date_naissance, date_deces) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (athleteId, bio["Full name"], sex, athleteCountry, taille, poids, born, died)
        )
        cursor.execute(
            "INSERT INTO medailles_athletes (athlete, sport, `or`, argent, bronze) VALUES (%s, %s, %s, %s, %s)",
            (athleteId, sportId, int(goldMedals), int(silverMedals), int(bronzeMedals))
        )
        
        
    #print(athKeys - athKeysOptional)
    #for key, values in athBioExamples.items():
    #    print(key, values)
    
    # tentative de récupération des scores avortée
    """match = re.search(r"<h2>Event types</h2>\s*<table[^>]+>\s*<thead>.*?</thead>\s*<tbody>(.*?)</tbody>\s*</table>", text, re.DOTALL)
    
    matches = re.findall(r"<tr valign='top'>\s*<td><a href=\"/event_names/([0-9]+)\">([^<]+)</a></td>\s*<td>([^<]+)</td>\s*<td><span class=\"glyphicon glyphicon-ok\"></span></td>", match.group(1))
    
    cursor.execute("SELECT nom FROM events WHERE sport = %s", sportId)
    events = [x[0] for x in cursor.fetchall()]
    events.sort()
    
    for event in events:
        print(event)

    for eventId, eventName, eventCategory in matches:
        get("event_names/" + eventId)
    """