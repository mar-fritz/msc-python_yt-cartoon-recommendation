import mysql.connector
from mysql.connector import errorcode
import getpass


def db_tables():
    """
    Return a dictionary of table creation queries where the key
    is the name of the table and the value is the query
    :return: dictionary of table creation queries
    """
    tables = {}

    # videos table stores video content information
    tables['videos'] = (
        "CREATE TABLE `videos` ("
        "  `id` varchar(11) NOT NULL,"
        "  `title` varchar(100) NOT NULL,"
        "  `url` varchar(35) NOT NULL,"
        "  `duration_sec` INT UNSIGNED NOT NULL,"
        "  `captions` varchar(50) NOT NULL,"
        "  PRIMARY KEY (`id`)"
        ") ENGINE=InnoDB")

    # statistics table holds video statistics
    tables['statistics'] = (
        "CREATE TABLE `statistics` ("
        "  `video_id` varchar(11) NOT NULL,"
        "  `date_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "  `view_count` BIGINT UNSIGNED NOT NULL,"
        "  `like_count` BIGINT UNSIGNED NOT NULL,"
        "  `dislike_count` BIGINT UNSIGNED NOT NULL,"
        "  CONSTRAINT `pk_statistics` PRIMARY KEY (`video_id`, `date_time`),"
        "  FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE"
        ") ENGINE=InnoDB")

    return tables


# Queries for data insertion
insert_video_query = ("INSERT INTO `videos` "
                      "(`id`, `title`, `url`, `duration_sec`, `captions`) "
                      "VALUES (%s, %s, %s, %s, %s)")

insert_statistics_query = ("INSERT INTO `statistics` "
                           "(`video_id`, `date_time`, `view_count`, `like_count`, `dislike_count`) "
                           "VALUES (%s, %s, %s, %s, %s)")


def db_connect(username, pwd, host="127.0.0.1", port="3306"):
    """
    Creates a connection to the MySQL database.
    Allows 3 attempts for entering credentials
    :param username: username
    :param pwd: password
    :param host: connection host
    :param port: connection port
    :return: connector object
    """
    # connect to the MySQL database (3 attempts for pwd)
    for attempt in range(0, 3):
        try:
            cnx = mysql.connector.connect(user=username, password=pwd,
                                          host=host, port=port
                                          )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your user name or password')
                user = input("Username:")
                pwd = getpass.getpass("Password for " + user + ":")
            else:
                raise err
        else:
            if cnx.is_connected():
                print("Successfully connected ", cnx)  # print a connection object
            break
    else:
        raise Exception("Too many incorrect tries!")
    # return connection object
    return cnx


def db_setup(cnx, dbname, tables):
    """
    Sets up the database needed for the application
    :param cnx: mysql connector object
    :param dbname: name of database to be created
    :param tables: dictionary of table creation queries
    :return: None
    """
    # create an instance of 'cursor' class
    cursor = cnx.cursor()
    # create a database called <dbname> if it doesn't already exist
    cursor.execute('CREATE DATABASE IF NOT EXISTS  {} '.format(dbname))
    # change to that database using the database property of the connection object
    cnx.database = dbname
    # create the tables
    for table_name in tables:
        table_description = tables[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print(table_name + " already exists.")
            else:
                print(err.msg)
        else:
            print(table_name + " OK")

    cursor.close()


def insert_data(cnx, query, data):
    """
    Inserts pandas DataFrame into the database row-by-row
    Rows which produce errors are skipped
    :param cnx: mysql connector object
    :param query: appropriate INSERT query
    :param data: pandas dataframe to be inserted
    :return: None
    """
    # create an instance of 'cursor' class
    cursor = cnx.cursor()

    # Insert DataFrame records one by one.
    for i, row in data.iterrows():
        try:
            cursor.execute(query, tuple(row))
        except mysql.connector.Error as err:
            print("There was an error during insertion of", row[0])
            print(err.msg)
            print("Row skipped")
    print("Data Insertion Complete")

    # Make sure data is committed to the database
    cnx.commit()
    cursor.close()
