import logging
import psycopg2
import os

from dotenv import load_dotenv
from typing import Union

from RabbitMQ.validators.command_validation import OperationCommand, ShiftCommand, ValidationError

load_dotenv()

logging.basicConfig(filename='loggers.log', level=logging.INFO, format='%(asctime)s:%(message)s')
logging.basicConfig(filename='loggers.log', level=logging.ERROR, format='%(asctime)s:%(message)s')


class DBCommand:
    __slots__ = ('__info', '__id_command', '__mode', '__connection', '__cursor', '__current_shift')

    def __init__(self, operation):
        # Передаваемая информация в формате JSON/словаря
        self.__info: Union[OperationCommand, ShiftCommand] = operation

        # Тут будем сравнивать коды и данные, чтобы создать Insert запросы
        self.__id_command = {
            10: 'db_tubetechoperations',  # Таблица техоперации
            20: 'db_storage',  # Таблица стеллажей
            30: 'db_shift',  # Таблица по регистрации смены
        }

        self.__mode = None

        # Если добавляется операция по трубам, то ещё нужно инициализировать режим работы
        if isinstance(self.__info, OperationCommand):
            """
            Here we check if we are dealing with tube_operations or with shift_operations
            """
            if self.__info.mode == 1:
                self.__mode = 'Автомат'
            else:
                self.__mode = 'Наладка'

        try:
            # Смотрим если соединение есть
            self.__connection = psycopg2.connect(database=os.getenv("SERVER_DATABASE"),
                                                 user=os.getenv("SERVER_USER"),
                                                 password=os.getenv("SERVER_PASSWORD"),
                                                 host=os.getenv("SERVER_HOST"),
                                                 port=os.getenv("SERVER_PORT"))
            self.__cursor = self.__connection.cursor()
            self.__connection.autocommit = True

        except psycopg2.Error as e:
            # В противном случае система выходит из класса
            logging.error(e)

        self.__current_shift = None

    async def __get_last_shift(self) -> any:
        """
        We get the last shift to skip the need of keeping track of the last shift
        :return: last shift in 'db_shift'
        """
        self.__cursor.execute("select id from db_shift order by time_date desc limit 1")
        for element in self.__cursor:
            self.__current_shift = element[0]
        return self.__current_shift

    # Набор функции для создания insert запроса в БД
    async def __insert_into_shift(self) -> None:
        """
        From the data we got, we insert them into the database 'db_shift'
        :return:
        """
        self.__cursor.execute(f"insert into {self.__id_command[self.__info.command_id]} (shiftnum, time_date) "
                              f"values ({self.__info.shift_num}, '{self.__info.datetime}') ")
        logging.info(f"Shift {self.__info.shift_num} added!")

    async def __insert_into_op(self) -> None:
        """
        From the data we got, we insert them into the database 'db_tubeoperations'
        :return:
        """
        self.__cursor.execute(
            f"insert into {self.__id_command[self.__info.command_id]} (opresult, optime, unit_regime, "
            f"unitref, shiftref) values ({self.__info.result}, '{self.__info.datetime}', {self.__mode}, "
            f"{self.__info.unit_id}, {self.__current_shift})")

        logging.info(f"{self.__info.unit_id}: {self.__info.result} databaseQuery created!")

    # async def insert_into_storage(self) -> None:
    #     try:
    #         self.cursor.execute(
    #             f"insert into {self.id_command[self.id]} (storage_ref, status, storage_time) "
    #             f"values ('{self.info['ID_UNIT']}', '{self.info['Value']}', '{self.info['DateTime']}')")
    #         logging.info(f"storage {self.info['ID_UNIT']} added!")
    #     except psycopg2.Error as Er:
    #         logging.error(Er)
    #     finally:
    #         self.cursor.close()
    #         self.connection.close()

    # Общая функция для наполнения при каждом случае
    async def insertIntoTable(self):
        """
        This function is created to ensure that we insert our data in the correct table.
        :return:
        """
        try:
            await self.__get_last_shift()
            if self.__info.command_id == 10:
                await self.__insert_into_shift()
            elif self.__info.command_id == 20:
                await self.__insert_into_op()
            # elif self.info.command_id == 30:
            #     await self.insert_into_storage()
            else:
                logging.error("Command out of bound! ")
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            self.__cursor.close()
            self.__connection.close()

    def __del__(self):
        del self.__info, self.__connection, self.__cursor, self.__id_command, self.__current_shift, self.__mode
