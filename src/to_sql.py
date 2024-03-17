import sqlite3
import pathlib
import json

def create_schema(c: sqlite3.Cursor):
    c.execute('''
        CREATE TABLE IF NOT EXISTS days (
            id INTEGER PRIMARY KEY,
            date TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS day_attributes (
            id INTEGER PRIMARY KEY,
            day_id INTEGER,
            class TEXT,
            text TEXT,
            FOREIGN KEY(day_id) REFERENCES days(id)
        )
    ''')

    # type is whether it is good or bad, True for good, False for bad
    c.execute('''
        CREATE TABLE IF NOT EXISTS attribute (
            id INTEGER PRIMARY KEY,
            day_id INTEGER,
            type INTEGER,
            value TEXT,
            FOREIGN KEY(day_id) REFERENCES days(id)
        )
    ''')

def filename_to_sqlite_date(filename: str) -> str:
    name = filename.split(".")[0]
    day, month, year = name.split("_")
    return f"{year}-{month}-{day}"

def parse_file_to_database(file_path: pathlib.Path, c: sqlite3.Cursor):
    with open(file_path, 'r') as f:
        data = json.load(f)

    c.execute('INSERT INTO days (date) VALUES (?)', (filename_to_sqlite_date(file_path.stem),))
    day_id = c.lastrowid

    for item in data['day_attributes']:
        c.execute('INSERT INTO day_attributes (day_id, class, text) VALUES (?, ?, ?)', (day_id, item['class'], item['text']))

    for type, values in data['good_bad_attributes'].items():
        type_bool = type == "good"
        for value in values:
            c.execute('INSERT INTO attribute (day_id, type, value) VALUES (?, ?, ?)', (day_id, type_bool, value))

def main():
    conn = sqlite3.connect('chinese_calendar.db')
    c = conn.cursor()
    create_schema(c)

if __name__ == "__main__":
    main()