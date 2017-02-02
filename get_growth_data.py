from datetime import datetime
import sqlite3

with sqlite3.connect("log.db") as db:
    db.row_factory = sqlite3.Row
    cursor = db.cursor()
    
    hrs_ago = 0
    points = []
    print "Time, Users, Sessions"
    while True:
        hrs_ago -= 1/60.0*60 * 12
        cursor.execute("select count(DISTINCT uuid) from user_sessions where timestamp < datetime('now', '{} hour');".format(hrs_ago))
        unique_users = cursor.fetchone()
        
        cursor.execute("select count(uuid) from user_sessions where timestamp < datetime('now', '{} hour');".format(hrs_ago))
        total_sessions = cursor.fetchone()
        
        cursor.execute("select datetime('now', '{} hour');".format(hrs_ago))
        ts = cursor.fetchone()
        
        print "{}, {}, {}".format(ts[0], unique_users[0], total_sessions[0])
        if int(total_sessions[0]) <= 0:
            break
    
    
    #for p in points:
     #   print "{}, {}\n".format(p[0], p[1])
