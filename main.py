import base64
from datetime import datetime
from pprint import pprint

import requests
from urllib.parse import quote
from dataclasses import dataclass

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Column, Table, JSON, Text, Numeric, String, DateTime, create_engine, MetaData, insert
from sqlalchemy.orm import sessionmaker


@dataclass
class Order:
    order_id: str
    account_id: str
    created: datetime
    delivery_planned_moment: datetime
    external_code: str
    name: str
    payed_sum: float
    shipment_address: str
    raw_response: dict


class DBSettings(BaseSettings):
    DATABASE_NAME: str = 'postgres'
    DATABASE_HOST: str = 'localhost'
    DATABASE_PORT: int = None
    DATABASE_USER: str = 'postgres'
    DATABASE_PASS: str = quote('password')

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='allow',
    )

    @property
    def DATABASE_URL(self):
        url = 'postgresql://{db_user}:{db_pass}@{db_host}'\
              ':{db_port}/{db_name}'

        return url.format(
            db_user=self.DATABASE_USER,
            db_pass=self.DATABASE_PASS,
            db_host=self.DATABASE_HOST,
            db_name=self.DATABASE_NAME,
            db_port=self.DATABASE_PORT,
        )


class SkladSettings(BaseSettings):
    LOGIN: str = None
    PASSVORD: str = None

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='allow',
    )


@dataclass
class DBIntefase:
    URL: str

    def _get_orders_table(self):

        metadata = MetaData()
        return Table(
            'orders',
            metadata,
            Column('order_id', String(255), primary_key=True),
            Column('account_id', String(255), nullable=False),
            Column('created', DateTime, nullable=False),
            Column('delivery_planned_moment', DateTime),
            Column('external_code', String(255)),
            Column('name', String(255), nullable=False),
            Column('payed_sum', Numeric(10, 2)),
            Column('shipment_address', Text),
            Column('raw_response', JSON, nullable=False),
        )

    def insert_order(self, orders: list[Order]):
        engine = create_engine(self.URL)

        orders_table = self._get_orders_table()
        data_to_insert = [order.__dict__ for order in orders]

        with engine.connect() as connection:
            try:
                stmt = orders_table.insert().values(data_to_insert)
                connection.execute(stmt)
                connection.commit()
                print("Данные успешно добавлены!")
            except Exception as e:
                connection.rollback()
                print("Ошибка при вставке данных:", e)


@dataclass
class MySkladIntefase:
    login: str
    password: str

    def get_orders(self):
        credentials = f"{self.login}:{self.password}"
        encoded_credentials = base64.b64encode(
            credentials.encode("utf-8")
        ).decode("utf-8")

        url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"

        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Accept-Encoding": "gzip",
        }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception('Проблемы с запросом заказов')

        date_format = '%Y-%m-%d %H:%M:%S.%f'

        out_list = []
        orders = response.json()

        for order in orders['rows']:
            order_data = Order(
                order_id=order['id'],
                account_id=order['accountId'],
                created=datetime.strptime(
                    order['created'],
                    date_format,
                ),
                delivery_planned_moment=datetime.strptime(
                    order['deliveryPlannedMoment'],
                    date_format,
                ),
                external_code=order['externalCode'],
                name=order['name'],
                payed_sum=order['payedSum'],
                shipment_address=order['shipmentAddress'],
                raw_response=order,
            )
            out_list.append(order_data)

        return out_list


@dataclass
class MySkladServise:
    sklad: MySkladIntefase
    database: DBIntefase

    def load_orders(self):
        orders = self.sklad.get_orders()
        self.database.insert_order(orders)


def main():
    db_settings = DBSettings()
    sklas_settings = SkladSettings()

    sklad_intefase = MySkladIntefase(
        login=sklas_settings.LOGIN, password=sklas_settings.PASSVORD
    )
    db_intefase = DBIntefase(URL=db_settings.DATABASE_URL)

    servise = MySkladServise(sklad_intefase, db_intefase)

    servise.load_orders()


if __name__ == "__main__":
    main()
