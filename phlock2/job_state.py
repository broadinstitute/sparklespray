# phlock job table:
#      phlockid, definition
#
# nomad job table:
#      phlock_job_id, nomad_job_id
#
# (just a cache, updated whenever new allocation appears:)
# task table:
#      phlock_job_id, phlock_task_id, allocation

import sqlite3
import os
import json

CREATE_SQL = ["CREATE TABLE job (id INTEGER PRIMARY KEY AUTOINCREMENT, name STRING, definition STRING)",
 "CREATE TABLE nomad_job (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, nomad_id STRING)"]

def open_db(filename):
    new_db = not os.path.exists(filename)

    c = sqlite3.connect(filename)
    if new_db:
        db = c.cursor()
        for stmt in CREATE_SQL:
            db.execute(stmt)
        db.close()

    return DB(c)

class Transaction:
    def __init__(self, c):
        self.c = c
    def __enter__(self):
        self.cursor = self.c.cursor()
        return self.cursor
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if exc_type is None:
            self.c.commit()
        else:
            self.c.rollback()

class DB:
    def __init__(self, c):
        self.c = c

    def tx(self):
        return Transaction(self.c)

    def add_job(self, name, definition):
        definition_json = json.dumps(definition)
        with self.tx() as db:
            db.execute("INSERT INTO job (name, definition) VALUES (?, ?)", [name, definition_json])
            return db.lastrowid

    def add_nomad_job_id(self, job_id, nomad_id):
        assert nomad_id is not None
        assert job_id is not None

        with self.tx() as db:
            db.execute("INSERT INTO nomad_job (job_id, nomad_id) VALUES (?, ?)", [job_id, nomad_id])


    def get_nomad_job_ids(self, job_id):
        with self.tx() as db:
            db.execute("SELECT nomad_id FROM nomad_job WHERE job_id = ?", [job_id])
            return [x[0] for x in db.fetchall()]

