import sqlite3
import pathlib
import json

from tqdm import tqdm

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
    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

def text_to_hex(text: str) -> str:
    return hex(ord(text))[2:]

def parse_file_to_database(file_path: pathlib.Path, c: sqlite3.Cursor):
    with open(file_path, 'r') as f:
        data = json.load(f)

    c.execute('INSERT INTO days (date) VALUES (?)', (filename_to_sqlite_date(file_path.stem),))
    day_id = c.lastrowid
    
    c.execute('SELECT date FROM days WHERE id = ?', (day_id,))
    date = c.fetchone()[0]

    for item in data['day_attributes']:
        c.execute('INSERT INTO day_attributes (day_id, class, text) VALUES (?, ?, ?)', (day_id, item['class'], text_to_hex(item['text'])))

    for type, values in data['good_bad_attributes'].items():
        type_bool = type == "good"
        for value in values:
            c.execute('INSERT INTO attribute (day_id, type, value) VALUES (?, ?, ?)', (day_id, type_bool, value))

def save_all_files_to_sql(c: sqlite3.Cursor):
    for file_path in tqdm(pathlib.Path("./results").glob("*.json")):
        parse_file_to_database(file_path, c)


def main():
    conn = sqlite3.connect('chinese_calendar.db')
    c = conn.cursor()
    create_schema(c)
    save_all_files_to_sql(c)
    conn.commit()

if __name__ == "__main__":
    main()