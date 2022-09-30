import datetime as dt
import json
import logging as log
import os

import psycopg2 as psy
from dotenv import load_dotenv

log.basicConfig(filename='shift.log', level=log.INFO, format='%(asctime)s:%(message)s')
load_dotenv()


def insert_into_shift(shift_num, time) -> None:
    """
    adds data to table 'db_shift' in the database
    :param shift_num: num in (0, 1) else error
    :param time: str type
    """
    try:
        connection = psy.connect(
            database=os.getenv("SERVER_DATABASE"),
            user=os.getenv("SERVER_USER"),
            password=os.getenv("SERVER_PASSWORD"),
            host=os.getenv("SERVER_HOST"),
            port=os.getenv("SERVER_PORT")
        )
        cursor = connection.cursor()
        cursor.execute(
            f"insert into db_shift (shiftnum, time_date) values ({shift_num}, '{time}')"
        )
        connection.commit()
        cursor.close()
        connection.close()
        log.info(f"shift {shift_num} added!")
    except psy.Error as e:
        log.error(e)


def add_shift() -> None:
    """
    Generates all needed data to add a new shift to 'db_shift'. current_time to indicate shift in (0, 1)
    """
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
        'SHIFT_NUM': shift_num,
        'DateTime': sql_insert_datetime
    }

    with open('../data.txt', 'a') as f:
        message = json.dumps(command)
        f.write(message)
        log.info(f'shift {shift_num} written in datatext!')


def main() -> None:
    """
    module to start the script
    :return:
    """
    add_shift()


if __name__ == '__main__':
    main()
