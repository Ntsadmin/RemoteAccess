import psycopg2
from psycopg2 import Error
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename='loggers.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logging.basicConfig(filename='loggers.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')


class Command:
    def __init__(self, json_request):
        # Передаваемая информация в формате JSON/словаря
        self.info = json_request

        # Тут будем сравнивать коды и данные, чтобы создать Insert запросы
        self.id_command = {
            10: 'db_tubetechoperations',  # Таблица техоперации
            20: 'db_storage',  # Таблица стеллажей
            30: 'db_shift',  # Таблица по регистрации смены

        }

        try:
            # Смотрим если соединение есть
            self.connection = psycopg2.connect(database=os.getenv("DATABASE"),
                                               user=os.getenv("USER"),
                                               password=os.getenv("PASSWORD"),
                                               host=os.getenv("HOST"),
                                               port=os.getenv("PORT"))

            self.cursor = self.connection.cursor()
            self.cursor.execute("SELECT id from db_shift order by id limit 1")
            for element in self.cursor:
                self.current_shift = element[0]

        except Error as e:
            # В противном случае система выходит из класса
            logging.error(e)

        # Инициализируем идентификатор команды
        if json_request['ID_CMD']:
            self.id = json_request['ID_CMD']

            for key in self.info:
                if key == 'Mode':
                    if self.info['Mode'] == 1:
                        self.mode = 'Автомат'
                    else:
                        self.mode = 'Наладка'

        else:
            # Если команда была сформирована некорректно, то программа останавливается
            logging.error("no command given! ")

    # Набор функции для создания insert запроса в БД
    async def insertIntoShift(self):
        try:
            self.cursor.execute(f"insert into {self.id_command[self.id]} (shiftnum, time_date) "
                                f"values ({self.info['Shiftnum']}, '{self.info['DateTime']}') ")
            logging.info(f"Shift {self.info['ShitNum']} added!")

        except psycopg2.Error as db_error:
            logging.error(db_error)

    async def insertIntoOp(self):
        try:
            self.cursor.execute(
                f"insert into {self.id_command[self.id]} (opresult, optime, unit_regime, unitref, shiftref) "
                f"values ({self.info['Result']}, '{self.info['DateTime']}', {self.mode}, {self.info['ID_UNIT']}, "
                f" {self.shift})")
            logging.info(f"{self.info['ID_UNIT']}: {self.info['Result']} databaseQuery created!")

        except psycopg2.Error as db_error:
            logging.error(db_error)

    async def insertIntoStorage(self):
        try:
            self.cursor.execute(
                f"insert into {self.id_command[self.id]} (storage_ref, status, storage_time) "
                f"values ('{self.info['ID_UNIT']}', '{self.info['Value']}', '{self.info['DateTime']}')")
            logging.info(f"storage {self.info['ID_UNIT']} added!")

        except psycopg2.Error as Er:
            logging.error(Er)

    # Общая функция для наполнения при каждом случае
    async def insertIntoTable(self):
        if self.id == 10:
            await self.insertIntoShift()
        elif self.id == 20:
            await self.insertIntoOp()
        elif self.id == 30:
            await self.insertIntoStorage()
        else:
            logging.error("Command out of bound! ")

        await self.connection.commit()
        await self.connection.close()

    def __del__(self):
        del self.info, self.id, self.id_command
