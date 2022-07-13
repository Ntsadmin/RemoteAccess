import datetime as dt
import json
import logging as log
import psycopg2 as psy
import os
from dotenv import load_dotenv

log.basicConfig(filename='shift.log', level=log.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
load_dotenv()


def insert_into_shift(shift_num, time):
    try:
        connection = psy.connect(
            database=os.getenv("SDATABASE"),
            user=os.getenv("SUSER"),
            password=os.getenv("SPASSWORD"),
            host=os.getenv("SHOST"),
            port=os.getenv("SPORT")
        )

        cursor = connection.cursor()
        cursor.execute(
            f"insert into db_shift (shiftnum, time_date) values ({shift_num}, '{time}')"
        )

        connection.commit()
        connection.close()

        log.info(f"shift {shift_num} added!")

    except psy.Error as e:
        log.error(e)


def add_shift():
    current_datetime = dt.datetime.now()
    current_time = current_datetime.time()

    if 8 <= current_time.hour < 20:
        shift_num = 1

    else:
        shift_num = 2

    sql_insert_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    insert_into_shift(shift_num, sql_insert_datetime)

    command = {
        'ID_CMD': 30,
        'Shiftnum': shift_num,
        'DateTime': sql_insert_datetime
    }

    with open('data.txt', 'a') as f:
        message = json.dumps(command)
        f.write(message)
        log.info(f'shift {shift_num} written in datatext!')


add_shift()
