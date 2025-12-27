import sqlite3

DB_NAME = "maintenance.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        email TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        password TEXT NOT NULL
    )''')

    # ADDED: purchase_date, warranty
    c.execute('''CREATE TABLE IF NOT EXISTS equipment (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category TEXT,
        serial TEXT UNIQUE,
        owner TEXT,
        team TEXT,
        status TEXT,
        health INTEGER,
        work_center TEXT,
        technician TEXT,
        purchase_date TEXT, 
        warranty TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject TEXT,
        maintenance_for TEXT,
        type TEXT,
        technician TEXT,
        team TEXT,
        date DATE,
        priority TEXT,
        stage TEXT DEFAULT 'New'
    )''')
    
    try:
        c.execute("INSERT INTO users VALUES (?, ?, ?)", ("admin@test.com", "Admin", "Admin@123"))
    except sqlite3.IntegrityError:
        pass 
    conn.commit()
    conn.close()

# --- User Functions ---
def create_user(name, email, password):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO users VALUES (?, ?, ?)", (email, name, password))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def verify_user(email, password):
    conn = get_connection()
    c = conn.execute("SELECT name FROM users WHERE email=? AND password=?", (email, password))
    user = c.fetchone()
    conn.close()
    return user[0] if user else None

# --- Request Functions ---
def add_request(subject, maintenance_for, r_type, tech, team, date, priority):
    conn = get_connection()
    conn.execute('''INSERT INTO requests (subject, maintenance_for, type, technician, team, date, priority, stage)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'New')''', 
                    (subject, maintenance_for, r_type, tech, team, date, priority))
    conn.commit()
    conn.close()

def get_all_requests():
    conn = get_connection()
    c = conn.execute("SELECT * FROM requests")
    cols = [description[0] for description in c.description]
    data = [dict(zip(cols, row)) for row in c.fetchall()]
    conn.close()
    return data

def update_request_stage(req_id, new_stage):
    conn = get_connection()
    conn.execute("UPDATE requests SET stage=? WHERE id=?", (new_stage, req_id))
    conn.commit()
    conn.close()

def get_request_count_for_equipment(eq_name):
    conn = get_connection()
    c = conn.execute("SELECT COUNT(*) FROM requests WHERE maintenance_for=? AND stage!='Repaired'", (eq_name,))
    count = c.fetchone()[0]
    conn.close()
    return count

# --- Equipment Functions ---
def add_equipment(data_dict):
    conn = get_connection()
    # Ensure keys exist for new fields if missing
    keys = ['name', 'category', 'serial', 'owner', 'team', 'status', 'health', 'work_center', 'technician', 'purchase_date', 'warranty']
    for k in keys:
        if k not in data_dict:
            data_dict[k] = "N/A"
            
    conn.execute('''INSERT INTO equipment (name, category, serial, owner, team, status, health, work_center, technician, purchase_date, warranty)
                    VALUES (:name, :category, :serial, :owner, :team, :status, :health, :work_center, :technician, :purchase_date, :warranty)''',
                    data_dict)
    conn.commit()
    conn.close()

def get_all_equipment():
    conn = get_connection()
    c = conn.execute("SELECT * FROM equipment")
    cols = [description[0] for description in c.description]
    data = [dict(zip(cols, row)) for row in c.fetchall()]
    conn.close()
    return data

def set_equipment_scrapped(eq_name):
    conn = get_connection()
    conn.execute("UPDATE equipment SET status='Scrapped', health=0 WHERE name=?", (eq_name,))
    conn.commit()
    conn.close()
