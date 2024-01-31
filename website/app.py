from flask import Flask, render_template, request
import pymysql
import unidecode
import re

app = Flask(__name__, static_url_path='/static')
app.config['TEMPLATES_AUTO_RELOAD'] = True

@app.context_processor
def process_host():
    if "para" in request.host:
        return {"title": "para", "extern_url": "https://olympics2024.nathaan.com", "extern_title": "Olympics"}
    else:
        return {"title": "o", "extern_url": "https://paralympics2024.nathaan.com", "extern_title": "Paralympics"}
    

def normalize(name):
    return re.sub("([^a-z0-9])\1*", "_", unidecode.unidecode(name).lower())
    
    
def get_picto_itineraire(agence, type):
    if agence == "RER":
        return "/static/rer.svg"
    else:
        return "/static/route_" + str(type) + ".png"
    

def connect_sql():
    if "para" in request.host:
        bdd = "paralympics"
    else:
        bdd = "olympics"
        
    return pymysql.connect(
        host="ssh.nathaan.com",
        user="user",
        password="pwd",
        database=bdd,
        port=3306
    )
    

@app.route('/')
def accueil():
    # page d'accueil
    return render_template('index.html')


@app.route('/sites')
def sites():
    # liste des sites
    conn = connect_sql()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, nom, latitude, longitude FROM sites ORDER BY nom ASC")
    rows = cursor.fetchall()
    
    sites = []
    for row in rows:
        sites.append({"id": row[0], "nom": row[1], "lat": row[2], "lon": row[3]})
    
    conn.close()
    
    return render_template('sites.html', sites=sites)


@app.route('/sites/<int:siteId>')
def site(siteId):
    # liste des sites
    
    conn = connect_sql()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, nom, latitude, longitude FROM sites WHERE id = %s", (siteId))
    row = cursor.fetchone()
    site = {"id": row[0], "nom": row[1], "lat": row[2], "lon": row[3]}
    
    # récupérer les sports Concernés
    cursor.execute("""
        SELECT DISTINCT sports.id, sports.nom
        FROM sports
        INNER JOIN sessions ON sessions.sport = sports.id
        WHERE sessions.site = %s
        ORDER BY sports.nom ASC
    """, (siteId,))
    
    sports = []
    for row in cursor.fetchall():
        sports.append({"id": row[0], "nom": row[1]})
    
    # récupérer chaque arrêt désservi par le site ainsi que les stations et itinéraires associés
    cursor.execute("""
        SELECT a.id, a.nom, sa.distance, st.id, st.nom, i.id, i.nom_court, i.nom_long, i.couleur, i.couleur_texte, i.type, ag.nom
        FROM sites s
        INNER JOIN sites_arrets sa ON sa.site = s.id
        INNER JOIN arrets a ON a.id = sa.arret
        INNER JOIN trajets_arrets ta ON ta.arret = a.id
        INNER JOIN trajets t ON t.id = ta.trajet
        INNER JOIN itineraires i ON i.id = t.itineraire
        INNER JOIN stations st ON st.id = a.station
        INNER JOIN agences ag ON i.agence = ag.id
        WHERE s.id = %s
    """, (siteId,))
    
    # pour le template
    itineraires = {}
    stations = {}
    
    # provisoire
    arrets = {}
    
    for arretId, arretNom, arretDist, stationId, stationNom, itineraireId, itineraireNomCourt, itineraireNomLong, itineraireCouleur, itineraireCouleurTexte, itineraireType, agence in cursor.fetchall():
        # on crée la station si besoin
        if stationId not in stations:
            stations[stationId] = {"id": stationId, "nom": stationNom, "arrets": [], "distance": [arretDist, arretDist]} # distance min et max
            
        # on crée l'arrêt
        if arretId not in arrets:
            arrets[arretId] = {"id": arretId, "nom": arretNom, "distance": arretDist, "itineraires": []}
            
        # on ajoute l'arrêt à la station
        if arrets[arretId] not in stations[stationId]["arrets"]:
            stations[stationId]["arrets"].append(arrets[arretId])
            stations[stationId]["distance"][0] = min(stations[stationId]["distance"][0], arretDist)
            stations[stationId]["distance"][1] = max(stations[stationId]["distance"][1], arretDist)
        
        # on crée l'itinéraire
        if itineraireId not in itineraires:
            itineraires[itineraireId] = {"id": itineraireId, "nomCourt": itineraireNomCourt, "nomLong": itineraireNomLong, "couleur": itineraireCouleur, "couleurTexte": itineraireCouleurTexte, "type": itineraireType, "agence": agence, "picto": get_picto_itineraire(agence, itineraireType)}
        
        # on ajoute l'itinéraire à l'arrêt
        if itineraires[itineraireId] not in arrets[arretId]["itineraires"]:
            arrets[arretId]["itineraires"].append(itineraires[itineraireId])
            
    for station in stations.values():
        station["arrets"].sort(key = lambda x: x["distance"])
        
    conn.close()
    
    return render_template(
        'site.html',
        sports=sports,
        site=site,
        stations=list(sorted(stations.values(), key=lambda x: x["distance"][0])),
        itineraires=list(itineraires.values())
    )


@app.route('/sports')
def sports():
    # liste des sports
    conn = connect_sql()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, nom FROM sports ORDER BY nom ASC")
    rows = cursor.fetchall()
    
    sports = []
    for row in rows:
        sports.append({"id": row[0], "nom": row[1]})
    
    conn.close()
    
    return render_template('sports.html', sports=sports)


@app.route('/sports/<string:sportId>')
def sport(sportId):
    # information sur sport
    conn = connect_sql()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, nom FROM sports WHERE id LIKE %s", (sportId,))
    row = cursor.fetchone()
    sport = {"id": row[0], "nom": row[1]}
    
    cursor.execute("""
        SELECT DISTINCT sites.id, sites.nom
        FROM sites
        INNER JOIN sessions ON sessions.site = sites.id
        WHERE sessions.sport LIKE %s
        ORDER BY sites.nom ASC
    """, (sportId,))
    
    sites = []
    for row in cursor.fetchall():
        sites.append({"id": row[0], "nom": row[1]})
        
    
    cursor.execute("""
        SELECT a.id, a.nom, m.`or`, m.argent, m.bronze, (m.`or` + m.argent + m.bronze) AS total
        FROM athletes a
        INNER JOIN medailles_athletes m ON m.athlete = a.id
        WHERE m.sport LIKE %s
        ORDER BY  total DESC, m.`or` DESC, m.`argent` DESC, m.`bronze` DESC
    """, (sportId,))
    
    athletes = []
    for row in cursor.fetchall():
        athletes.append({"id": row[0], "nom": row[1], "or": row[2], "argent": row[3], "bronze": row[4]})
        
    conn.close()
    
    return render_template('sport.html', sport=sport, sites=sites, athletes=athletes)


@app.route('/calendrier')
def calendrier():
    # calendrier
    conn = connect_sql()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sp.id, sp.nom, se.numero, se.debut, se.fin, si.id, si.nom, j.`date`, ev.nom
        FROM sports sp
        INNER JOIN sessions se ON se.sport = sp.id
        INNER JOIN jours j ON j.id = se.jour
        INNER JOIN `events` ev ON ev.sport = se.sport AND ev.session = se.numero
        INNER JOIN sites si ON si.id = se.site
        ORDER BY sp.id ASC, se.numero ASC, ev.nom ASC
    """)
    
    sports = {}
    
    for sportId, sportNom, sessionNumero, sessionDebut, sessionFin, siteId, siteNom, date, eventNom in cursor.fetchall():
        if sportId not in sports:
            sports[sportId] = {"nom": sportNom, "sessions": {}}
            
        if sessionNumero not in sports[sportId]["sessions"]:
            sports[sportId]["sessions"][sessionNumero] = {"numero": sessionNumero, "debut": sessionDebut, "fin": sessionFin, "date": date, "events": [], "site": {"id": siteId, "nom": siteNom}}
            
        sports[sportId]["sessions"][sessionNumero]["events"].append(eventNom)
        
    sports = list(sorted(sports.values(), key = lambda x: x["nom"]))
    for sport in sports:
        sport["sessions"] = list(sorted(sport["sessions"].values(), key = lambda x: x["numero"]))
    
    return render_template('calendrier.html', sports=sports)


@app.route('/athletes/<int:athleteId>')
def athlete(athleteId):
    # info sur athlète
    conn = connect_sql()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, nom, genre, taille, poids, date_naissance, date_deces, nom_pays
        FROM athletes
        INNER JOIN pays ON athletes.nationalite = pays.iso
        WHERE id = %s
    """, (athleteId,))
    
    row = cursor.fetchone()
    athlete = {"id": row[0], "nom": row[1], "genre": "Homme" if row[2] == "M" else "Femme", "taille": row[3], "poids": row[4], "naissance": row[5], "deces": row[6], "pays": row[7]} 
    conn.close()
    
    return render_template('athlete.html', athlete=athlete)