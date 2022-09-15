import psycopg2
import json
from pprint import pprint

DB_NAME = 'clients_db'
DB_USER = 'postgres'


def main():
    with open('info_not_for_git/postgresql/users/postgres.json') as f:
        f_data = json.load(f)
    db_user_pass = f_data['pass']

    with psycopg2.connect(database=DB_NAME, user=DB_USER, password=db_user_pass) as db_conn:
        create_db(db_conn)
        add_new_client(db_conn, 'Алексей', 'Осипов', phones=['+79054001824'], emails=['osip.a@ya.ru'])
        add_new_client(db_conn, '@@@@Владимир', 'Парнет')
        add_new_client(db_conn, 'Владимир', 'Беляков', emails=['bel@jci.com'])
        add_new_client(db_conn, 'Ярослав', 'Кудинов', phones=['ytttttt'], emails=['kud.ya@hts.ru'])
        add_new_client(db_conn, 'Николай', 'Староверов', phones=['+79050121420', '+79261114455'], emails=['starov@jci.com'])
        add_phone_to_client(db_conn, 4, '+7787277')
        update_client_info(db_conn, 9, phones=['+79156', '+791178'])
        delete_phone_from_client(db_conn, 4)
        delete_client(db_conn, 9)
        pprint(find_client(db_conn, first_name='%ник%', phone='%926%'))


def create_db(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS clientinfo (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            CONSTRAINT proper_first_name CHECK (first_name ~* '^[A-Za-zА-Яа-я]{2,}$'),
            CONSTRAINT proper_last_name CHECK (last_name ~* '^[A-Za-zА-Яа-я]{2,}$')
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS phone (
            client_id INTEGER NOT NULL REFERENCES clientinfo,
            phone VARCHAR(20) NOT NULL,
            PRIMARY KEY (client_id, phone),
            CONSTRAINT proper_phone CHECK (phone ~ '^[+]{1}\d{3,}$')
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS email (
            client_id INTEGER NOT NULL REFERENCES clientinfo,
            email VARCHAR(50) NOT NULL,
            PRIMARY KEY (client_id, email),
            CONSTRAINT proper_email CHECK (email ~* '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$')
        );
        """)
        conn.commit()


def add_new_client(conn, first_name, last_name, phones=None, emails=None):
    with conn.cursor() as cursor:
        try:
            cursor.execute("""
            INSERT INTO clientinfo (first_name, last_name) VALUES 
                (%s, %s)
            RETURNING id;
            """, (first_name, last_name))
            client_id = cursor.fetchone()[0]
        except psycopg2.DatabaseError as err:
            print(f'{err.pgcode}: {err.pgerror}')
        conn.commit()
        if phones:
            try:
                cursor.execute("""
                INSERT INTO phone (client_id, phone)
                SELECT %s, unnest(array[%s]);
                """, (client_id, phones))
            except psycopg2.DatabaseError as err:
                print(f'{err.pgcode}: {err.pgerror}')
            conn.commit()
        if emails:
            try:
                cursor.execute("""
                INSERT INTO email (client_id, email)
                SELECT %s, unnest(array[%s]);
                """, (client_id, emails))
            except psycopg2.DatabaseError as err:
                print(f'{err.pgcode}: {err.pgerror}')
            conn.commit()


def add_phone_to_client(conn, client_id, phone):
    with conn.cursor() as cursor:
        try:
            cursor.execute("""
            INSERT INTO phone (client_id, phone) VALUES
                (%s, %s);
            """, (client_id, phone))
        except psycopg2.DatabaseError as err:
            print(f'{err.pgcode}: {err.pgerror}')
        conn.commit()


def update_client_info(conn, client_id, first_name=None, last_name=None, phones=None, emails=None):
    with conn.cursor() as cursor:
        try:
            if first_name and not last_name:
                cursor.execute("""
                UPDATE clientinfo 
                SET first_name = %s
                WHERE id = %s;
                """, (first_name, client_id))
            elif last_name and not first_name:
                cursor.execute("""
                UPDATE clientinfo 
                SET last_name = %s
                WHERE id = %s;
                """, (last_name, client_id))
            elif first_name and last_name:
                cursor.execute("""
                UPDATE clientinfo 
                SET first_name = %s, last_name = %s
                WHERE id = %s;
                """, (first_name, last_name, client_id))
        except psycopg2.DatabaseError as err:
            print(f'{err.pgcode}: {err.pgerror}')
        conn.commit()
        # "грубый", но быстрый метод обновления номеров телефонов: удаляем ВСЕ старые и добавляем ВСЕ новые.
        # для более "тонкой" работы нужно удалять и добавлять номера телефонов по одному, используя функции
        # "delete_phone_from_client" и "add_phone_to_client"
        if phones:
            try:
                cursor.execute("""
                DELETE FROM phone
                WHERE client_id = %s;
                """, (client_id,))
                cursor.execute("""
                INSERT INTO phone (client_id, phone)
                SELECT %s, unnest(array[%s]);
                """, (client_id, phones))
            except psycopg2.DatabaseError as err:
                print(f'{err.pgcode}: {err.pgerror}')
            conn.commit()
        # "грубый", но быстрый метод обновления email: удаляем ВСЕ старые и добавляем ВСЕ новые
        # для более "тонкой" работы нужно удалять и добавлять email по одному, используя другие функции
        if emails:
            try:
                cursor.execute("""
                DELETE FROM email
                WHERE client_id = %s;
                """, (client_id,))
                cursor.execute("""
                INSERT INTO email (client_id, email)
                SELECT %s, unnest(array[%s]);
                """, (client_id, emails))
            except psycopg2.DatabaseError as err:
                print(f'{err.pgcode}: {err.pgerror}')
            conn.commit()


def delete_phone_from_client(conn, client_id):
    query = "DELETE FROM phone WHERE client_id=%s"
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (client_id,))
        except psycopg2.DatabaseError as err:
            print(f'{err.pgcode}: {err.pgerror}')
        conn.commit()


def delete_client(conn, client_id):
    del_phone_query = "DELETE FROM phone WHERE client_id=%s"
    del_email_query = "DELETE FROM email WHERE client_id=%s"
    del_client_query = "DELETE FROM clientinfo WHERE id=%s"
    with conn.cursor() as cursor:
        try:
            cursor.execute(del_phone_query, (client_id,))
            cursor.execute(del_email_query, (client_id,))
            cursor.execute(del_client_query, (client_id,))
        except psycopg2.DatabaseError as err:
            print(f'{err.pgcode}: {err.pgerror}')
        conn.commit()


def find_client(conn, first_name=None, last_name=None, phone=None, email=None):
    base_query = """
    SELECT id, first_name, last_name, phone, email FROM clientinfo c
    JOIN phone p ON p.client_id = c.id
    JOIN email e ON e.client_id = c.id"""

    filter_args = []
    filter_args_var = []
    if first_name:
        filter_args.append('first_name')
        filter_args_var.append(first_name)
    if last_name:
        filter_args.append('last_name')
        filter_args_var.append(last_name)
    if phone:
        filter_args.append('phone')
        filter_args_var.append(phone)
    if email:
        filter_args.append('email')
        filter_args_var.append(email)

    filter_q = ""
    if filter_args:
        filter_q += " WHERE "
        for itm in filter_args:
            filter_q += itm + " iLIKE %s AND "
        filter_q = filter_q[:-5] + ";"
    else:
        filter_q += ";"

    query = base_query + filter_q
    # print(query)
    # print(*filter_args)
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (*filter_args_var,))
            return cursor.fetchall()
        except psycopg2.DatabaseError as err:
            print(f'{err.pgcode}: {err.pgerror}')


if __name__ == '__main__':
    main()
