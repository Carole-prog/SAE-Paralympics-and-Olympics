"""
Olympic Games 2024
Schedule Parser
"""

import xml.etree.ElementTree as ET
import re
import pymysql

# regular expressions
REGEX1 = re.compile(r"^Session\s+([A-Za-z0-9]{3})\s*([0-9]{1,2})\s+Start\s*:?\s+([0-9]{2}:[0-9]{2})\s+(?:End|Fin)\s*:\s+([0-9]{2}:[0-9]{2})\s+(.*)\s*$")
REGEX2 = re.compile(r"^(?:Day|DAY)\s+(-?[0-9]+)\s+([A-Za-z]+)\s+([0-9]+)\s+([A-Za-z]+)$")

# strings to ignore
IGNORE = [
    "The competition schedule is subject to change until the conclusion of the Olympic Games.",
    "The competition schedule is subject to change until the conclusion of the Paralympic Games.",
    "The competition schedule is subject to change until the conclusion of the Games.",
    "The information below includes all possible events for this court. The final court assignments and order of play are subject to change up until the day of the session",
    "The information provided is focused on the content of the sessions, not the timings nor the order of the",
    "events. More detailed information will be provided at a later stage.",
    "Schedule will be adjusted based on the number of athletes per weigh category that will be confirmed in the Qualification System to be",
    "submitted for validation by the IOC Executive Board in September",
    "Note that the session below are sport sessions and not necessarly ticketed sessions.",
    "Note that the events schedule below is subject to change as there will be a daily process to decide on order of play depending on player",
    "rest time, TV requests and other considerations (which obviously is not possible to forecast at this point)"
]

# fixes for some locations; most of the names are from
# the first table (the calendar for every location)
LOCATIONS_FIXES = {
    "La Concorde": "La Concorde 2", # real name in the calendar, first pages
    "Pierre Mauroy Stadium*": "Pierre Mauroy Stadium", # removing the star
    "Chateauroux Shooting Centre*": "Chateauroux Shooting Centre", # removing the star
    "North Paris Arena*": "North Paris Arena", # removing the star,
    "Start Invalides - Finish Pont Alexandre III": "Invalides", # keeping only start venue
    "Hôtel de Ville (Start Venue) - Invalides (Finish Venue)": "Hôtel de Ville", # keeping only start venue
    
    "Roland-Garros Stadium - Philippe Chatrier": "Roland-Garros Stadium - P. Chartrier",
    "Roland-Garros Stadium - Court Philippe-Chatrier": "Roland-Garros Stadium - P. Chartrier",
    "Roland-Garros Stadium Tennis Park - Ph. Chatrier": "Roland-Garros Stadium - P. Chartrier",
    "Roland-Garros Stadium Tennis Park - Ph.Chatrier": "Roland-Garros Stadium - P. Chartrier",
    "Roland-Garros Stadium Tennis Park - S. Lenglen": "Roland-Garros Stadium - S. Lenglen",
    "Roland-Garros Stadium - Court Suzanne Lenglen*": "Roland-Garros Stadium - S. Lenglen",
    "Roland-Garros Stadium - Court Suzanne-Lenglen*": "Roland-Garros Stadium - S. Lenglen",
    "Roland-Garros Stadium - Court Suzanne-Lenglen": "Roland-Garros Stadium - S. Lenglen",
    "Roland-Garros Stadium Tennis Park - S.Matthieu": "Roland-Garros Stadium - S. Matthieu",
    
    "Arena Porte de la Chapelle": "Porte de la Chapelle Arena",
    "Porte de La Chapelle Arena": "Porte de la Chapelle Arena",
    
    "Clichy-sous-Bois*": "Clichy-sous-Bois",
    "Champ de Mars Arena": "Champs de Mars Arena",
    
    "Le Bourget Sport Climbing venue": "Le Bourget Sport Climbing Venue",
    "Marseille Marina": "Marina Marseille",
    
    "Vaires-sur-Marne Nautical Stadium": "Vaires-sur-Marne Nautical Stadium - Flatwater",
    "Vaires-Sur-Marne Nautical Stadium - Flatwater": "Vaires-sur-Marne Nautical Stadium - Flatwater",
    
    "Velodrome National": "Saint-Quentin-en-Yvelines Velodrome" # okay
}

class Sport:
    def __init__(self, name):
        self.name = name
        self.days = []
        
        
class Day:
    def __init__(self, number):
        self.number = number
        self.sessions = []
        
        
class Session:
    def __init__(self, name, start, end, location):
        self.name = name
        self.start = start
        self.end = end
        self.location = location
        
        self.events = []



# parse the xml file
tree = ET.parse('para_pdf.xml') # xml generated by pdfminer - `pdf2txt.py -t xml OLY_Competition-schedule-V3.1.pdf`
root = tree.getroot()

meta = []
state = 0

sports = {}

currentSport = None
currentDay = None
currentSession = None


def saveMeta():
    global meta, currentSession
    
    if meta:
        if currentSession:
            if currentSession.events:
                print(currentSession.events, meta)
                raise Exception("rewriting events")
                
            currentSession.events = meta.copy()
        
        else:
            raise Exception("data, but no current session")
        
    meta.clear()
    

def handleLine(line):
    global meta, state, currentSport, currentDay, currentSession
    print(line)
    
    if state == 1:
        # expect version
        if not line.startswith("Version"):
            raise Exception(line)
            
        state = 0
        return
    
    for txt in IGNORE:
        if line == txt:
            return
            
    # now we're playing it safe
    for txt in IGNORE:
        if txt in line:
            raise Exception(txt)
        
    if "Session" in line:
        match = REGEX1.match(line)
        if not match:
            raise Exception("Line contains 'Session', but the regular expression does not match")
            
        saveMeta()
        
        sessionName, sessionNum, start, end, location = match.groups()
        
        if location in LOCATIONS_FIXES:
            location = LOCATIONS_FIXES[location]
            
        session = sessionName.upper() + str(sessionNum).rjust(2, "0")
        
        currentSession = Session(session, tuple(int(i) for i in start.split(":")), tuple(int(i) for i in end.split(":")), location)
        currentDay.sessions.append(currentSession)
        
        
    elif "Event name" in line:
        if line.strip() != "Event name":
            raise Exception("Parse error. Expected 'Event name', got more data")
            
        if meta:
            raise Exception("expected no meta!")
        
    elif line.lower().startswith("day"):
        match = REGEX2.match(line)
        if not match:
            raise Exception("regex does not match for day")
            
        saveMeta()
        
        calendarDay, dayName, dayNumber, monthName = match.groups()
        print(calendarDay, dayName, dayNumber, monthName)
        
        currentDay = Day(int(calendarDay))
        currentSession = None
        
        currentSport.days.append(currentDay)
        
        
    elif "Competition Schedule Event Details" in line:
        if line != "Competition Schedule Event Details":
            raise Exception("line isnt equals to whats expected")
            
        state = 1 # expect version
        sport = meta.pop()
        
        if not (currentSport and currentSport.name == sport):
            # new sport!
            saveMeta()
            
            if sport in sports:
                raise Exception("sport already exists, not overwriting")
            
            currentSport = Sport(sport)
            currentDay = None
            currentSession = None
        
            sports[sport] = currentSport
        
    else:
        meta.append(line)
        #print(line)

for page in root[3:]:
    lines = {}
    
    for textbox in page:
        if textbox.tag != "textbox":
            continue
            
        for textline in textbox:
            bbox = [round(float(x)) for x in textline.get("bbox").split(",")]
            lineId = bbox[3]
            
            text = ""
            for elt in textline:
                text += elt.text
                
            if text.strip() == "#C1-INTERNAL":
                continue
                
            if "#C1-INTERNAL" in text:
                raise Exception("C1 internal not handled correctly")
                
            for otherId in lines:
                if abs(lineId - otherId) <= 2:
                    lineId = otherId
                    break
                    
            else:
                lines[lineId] = []
                
            lines[lineId].append((bbox[2], text))
            
    for height, line in sorted(lines.items(), reverse=True):
        line.sort()
        line = " ".join(x[1].strip() for x in line)
        line = line.strip()
        if line:
            handleLine(line)
            
            
# last check, we have parsed everything
for sport in sports.values():
    prev = None
    for day in sport.days:
        if prev and day.number <= prev.number:
            raise Exception("something went wrong - day number is incorrect")
        
        prev2 = None
        for session in day.sessions:
            if prev2 and int(session.name[3:]) <= int(prev2.name[3:]):
                raise Exception("something went wrong - session number is incorrect (%r, %r)" % (session.name, prev2.name))
                
            prev2 = session
        
        prev = day


# if we got past this, then it is gud ^w^

locationsIds = {}

conn = pymysql.connect(
    host="ssh.nathaan.com",
    user="user",
    password="pwd",
    database="olympics",
    port=3306,
    autocommit=False
)

cursor = conn.cursor()
cursor.execute("DELETE FROM sessions")
cursor.execute("DELETE FROM events")

cursor.execute("SELECT id, nom FROM sites")
for siteId, siteNom in cursor.fetchall():
    locationsIds[siteNom] = siteId
    
for sport in sports.values():
    #print()
    #print()
    print(sport.name)
    #print('("%s", "%s"),' % (sport.days[0].sessions[0].name[:3], " ".join(x.capitalize() for x in sport.name.split())))
    
    sportId = sport.days[0].sessions[0].name[:3]
    for day in sport.days:
        for session in day.sessions:
            if session.name[:3] != sportId:
                raise Exception("invalid session code")
    
    for day in sport.days:
        print("  Day %d" % day.number)
        for session in day.sessions:
            start = "%02d:%02d:00" % session.start
            end = "%02d:%02d:00" % session.end
            
            if session.location not in locationsIds:
                raise Exception("missing location %r" % session.location)
                
            cursor.execute("INSERT INTO sessions (sport, numero, jour, debut, fin, site) VALUES (%s, %s, %s, %s, %s, %s)", (session.name[:3], int(session.name[3:]), day.number, start, end, locationsIds[session.location]))
            
            for event in session.events:
                cursor.execute("INSERT INTO events (nom, sport, session) VALUES (%s, %s, %s)", (event, session.name[:3], int(session.name[3:])))
            
            #print("    -> Session %s - from %02d:%02d to %02d:%02d - %s" % (session.name, *session.start, *session.end, session.location))
            
            #for event in session.events:
                #print("      -> %s" % event)

conn.commit()
