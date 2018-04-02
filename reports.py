from mysql.connector import MySQLConnection, Error
from configparser import ConfigParser
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from os.path import basename
import os
import datetime
import pandas as pd
import smtplib
import json


def read_db_config(filename='<default_config_file_path>', section='mysql'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # get mysql section
    return read_config(filename, section)


def read_email_config(filename='<default_config_file_path>', section='email'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of email configuration
    :return: a dictionary of email parameters
    """
    # get email section
    return read_config(filename=filename, section=section)


def read_config(filename, section):
    """ create parser and read ini configuration file
        :param filename: name of the configuration file
        :param section: section of email configuration
        :return: a dictionary of config parameters
        """
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            config[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return config


def get_queries():
    """ function to read queries.json file to read all the query objects from the given json file in the path
     initializing an object with two kinds of reports that may exist in parsing queries.json """
    all_queries = {"DAILY": [], "WEEKLY": []}
    read_data = ""
    with open('<path to queeris.json file>') as json_data:
        for data in json_data:
            read_data = read_data + data
    queries = json.loads(read_data)
    for query in queries:
        all_queries[query["type"]].append(query)  # adding to the all_queries object
    return all_queries


def connect():
    """ Connect to MySQL database """
    db_config = read_db_config()
    conn = None
    try:
        print('Connecting to MySQL database...')
        conn = MySQLConnection(**db_config)

        if conn.is_connected():
            print('connection established.')
            all_queries = get_queries()
            daily_reports = all_queries["DAILY"]
            weekly_reports = all_queries["WEEKLY"]
            # run weekly reports if today is Monday according to the calendar date
            # similarly this can be augmented to run monthly reports as well
            if datetime.date.today().weekday() == 0:
                run_reports(weekly_reports, conn)
            run_reports(daily_reports, conn)  # run daily irrespective of the date
        else:
            print('connection failed.')

    except Error as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Connection closed.')


def run_reports(reports, conn):
    for query in reports:
        run_report(connection=conn, query_obj=query)


def get_query_str(query_arr):
    """ join the query array to produce a single query string,
     hence while creating a queries.json spaces are to be taken care of, otherwise the mysql syntax may fail
     or you can join over space character to avoid errors in the code as indicated below
     :param query_arr: is a array of strings containing sub strings to query
     :return query: joined query string over the white space character """
    query = " ".join(query_arr)
    return query


def run_report(connection, query_obj):
    """ function that drives everything, querying a database connection with the query
     then generating pandas data frame with the result set
     and then converting the data frame into csv and sending it to the recipients over mail
     :param connection: to mysql database
     :param query_obj: which contains configuration to the report to be run
    """
    cursor = connection.cursor()
    querystr = get_query_str(query_obj['query'])
    cursor.execute(querystr)
    result_set = list(cursor.fetchall())
    if len(result_set) > 0:
        frame = pd.DataFrame(result_set, columns=query_obj['headers'])
        last_date = datetime.datetime.today() - datetime.timedelta(days=1)
        last_date = last_date.strftime("%d-%m-%Y")
        subject = query_obj['subject'] + ": " + last_date
        filename = "<path_to_parent _directory>" + query_obj['filename'] + last_date + ".csv"
        frame.to_csv(filename, sep=',', encoding='utf-8', index=False)
        send_mail(send_to=query_obj["to"], subject=subject, filename=filename)
        os.remove(filename)  # remove file after successful delivery of mail


def send_mail(send_to, subject, text=None, filename=None, server="<smtp server host>"):
    """
    Function to send mail to the desired recipient by attaching report in csv format
    :param send_to: recipient email addresses
    :param subject: subject of the mail
    :param text: message body
    :param filename: filename of the csv file to be sent
    :param server: smtp host name of the mailing server
    :return:
    """
    email_config = read_email_config()
    msg = MIMEMultipart()
    msg['From'] = email_config['user']
    msg['To'] = COMMASPACE.join(send_to) if type(send_to) is list else send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    if text is not None:
        msg.attach(MIMEText(text))

    with open(filename, "rb") as fil:
        part = MIMEApplication(
            fil.read(),
            Name=basename(filename)
        )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(filename)
        msg.attach(part)

    smtp = smtplib.SMTP(server, 587)
    smtp.ehlo()
    smtp.starttls()
    smtp.login(email_config['user'], email_config['password'])
    smtp.sendmail(email_config['user'], send_to, msg.as_string())
    smtp.quit()
    smtp.close()


if __name__ == '__main__':
    connect()  # starting point for this script
