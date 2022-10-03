import logging
import psycopg2
import os

from dotenv import load_dotenv
from typing import Union

from RabbitMQ.validators.command_validation import OperationCommand, ShiftCommand, ValidationError

load_dotenv()

logging.basicConfig(filename='Rabbitmq_receiver.log', level=logging.INFO, format='%(asctime)s:%(message)s')
logging.basicConfig(filename='Rabbitmq_receiver.log', level=logging.ERROR, format='%(asctime)s:%(message)s')


class DBCommand:
    __slots__ = ('info', 'id_command', 'mode', 'connection', 'cursor', 'current_shift')

    def __init__(self, operation):
        # Передаваемая информация в формате JSON/словаря
        self.info: Union[OperationCommand, ShiftCommand] = operation

        # Тут будем сравнивать коды и данные, чтобы создать Insert запросы
        self.id_command = {
            10: 'db_tubetechoperations',  # Таблица техоперации
            20: 'db_storage',  # Таблица стеллажей
            30: 'db_shift',  # Таблица по регистрации смены
        }

        self.mode = None

        # Если добавляется операция по трубам, то ещё нужно инициализировать режим работы
        if isinstance(self.info, OperationCommand):
            """
            Here we check if we are dealing with tube_operations or with shift_operations
            """
            if self.info.mode == 1:
                self.mode = 'Автомат'
            else:
                self.mode = 'Наладка'

        try:
            # Смотрим если соединение есть
            self.connection = psycopg2.connect(database=os.getenv("SERVER_DATABASE"),
                                               user=os.getenv("SERVER_USER"),
                                               password=os.getenv("SERVER_PASSWORD"),
                                               host=os.getenv("SERVER_HOST"),
                                               port=os.getenv("SERVER_PORT"))
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True

        except psycopg2.Error as e:
            # В противном случае система выходит из класса
            logging.error(e)

        self.current_shift = None

    async def get_last_shift(self) -> any:
        """
        We get the last shift to skip the need of keeping track of the last shift
        :return: last shift in 'db_shift'
        """
        self.cursor.execute("select id from db_shift order by time_date desc limit 1")
        for element in self.cursor:
            self.current_shift = element[0]
        return self.current_shift

    # Набор функции для создания insert запроса в БД
    async def insert_into_shift(self) -> None:
        """
        From the data we got, we insert them into the database 'db_shift'
        :return:
        """
        self.cursor.execute(f"insert into {self.id_command[self.info.command_id]} (shiftnum, time_date) "
                            f"values ({self.info.shift_num}, '{self.info.datetime}') ")
        logging.info(f"Shift {self.info.shift_num} added!")

    async def insert_into_op(self) -> None:
        """
        From the data we got, we insert them into the database 'db_tubeoperations'
        :return:
        """
        self.cursor.execute(
            f"insert into {self.id_command[self.info.command_id]} (opresult, optime, unit_regime, "
            f"unitref, shiftref) values ({self.info.result}, '{self.info.datetime}', {self.mode}, "
            f"{self.info.unit_id}, {self.current_shift})")

        logging.info(f"{self.info.unit_id}: {self.info.result} databaseQuery created!")

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
            await self.get_last_shift()
            if self.info.command_id == 30:
                await self.insert_into_shift()
            # elif self.info.command_id == 20:
            #     await self.insert_into_storage()
            elif self.info.command_id == 10:
                await self.insert_into_op()
            else:
                logging.error("Command out of bound! ")
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            self.cursor.close()
            self.connection.close()

    def __del__(self):
        del self.info, self.id_command, self.connection, self.cursor, self.current_shift, self.mode
