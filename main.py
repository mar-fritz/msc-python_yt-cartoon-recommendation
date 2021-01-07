import mysql.connector
import db_operations


if __name__ == '__main__':
    print("start program")
    try:
        # Connect to mysql server
        cnx = db_operations.db_connect("root", "x", "127.0.0.1", "3306")
        # Create db and tables
        db_operations.db_setup(cnx, 'test', db_operations.db_tables())
    except mysql.connector.Error as err:
        print(err)
        print("setup failed")
    else:
        print("setup finished")
        # close the connection
        cnx.close()
