import time
import pymysql
import sys
import serial
import logging
import math
import configparser
from twilio.rest import Client

# Set some parameters for the log file
logger = logging.getLogger("WellStatus")
logger.setLevel(logging.INFO)
fh = logging.FileHandler("/home/crewsr/python/WellStatus.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.info("Program 'WellStatus' started")

# set up the serial port
ser = None
try:
    ser = serial.Serial('/dev/ttyACM0', baudrate=9600)
except serial.SerialException:
    print("Could not connect to serial device: please ensure that Arduino is connected")

# get configs
config = configparser.ConfigParser()
config.read("./settings.ini")
account_sid = config.get('Twilio', 'AccountSid')
auth_token = config.get('Twilio', 'AuthToken')
from_num = config.get('Twilio', 'FromNum')
phone_num = config.get('Twilio', 'PhoneNum')
user_name = config.get('MySql', 'UserName')
password = config.get('MySql', 'Password')
ip_address = config.get('MySql', 'IpAddress')
database_name = config.get('MySql', 'DatabaseName')


# Create the sms client object
# smsClient = Client(account_sid, auth_token)
# smsMessage = smsClient.messages.create(
#     body="Well Status program activated.  Water pressure is being monitored",
#     from_=from_num,
#     to=phone_num
#     )
# print(smsMessage.sid)

rowCount = 0
count = 59
tempC = 0
pressRead = 0
humidity = 0
newStart = True


# Function to calculate temperature from
# TMP36 data, rounded to specified
# number of decimal places.
def convert_temp(temp_data, places):
    temp_f = (float(temp_data) * 9 / 5) + 32
    temp_f = round(temp_f, places)
    #    print (temp_f)
    return temp_f


def convert_press(press_data, places):
    adjusted_read = press_data - 102
    max_read = adjusted_read * 150
    press = max_read / 819
    press = press + 17
    press = round(press, places)
    return press


# Function to push informational messages to the message table
def log_info(message):
    cur_time = time.strftime('%Y-%m-%d %H:%M:%S')
    msg_type = "INFO"
    try:
        record = [message, cur_time, msg_type]
        cursor.execute("INSERT INTO messages (message,evttime,msgtype) VALUES (%s,%s,%s)", record)
        db.commit()
    except pymysql.Error:
        logger.error("Could not add info message to message database")
        db_connect()


# Function to push error messages to the message table
def log_error(message):
    cur_time = time.strftime('%Y-%m-%d %H:%M:%S')
    msg_type = "ERROR"
    try:
        record = [message, cur_time, msg_type]
        cursor.execute("INSERT INTO messages (message,evttime,msgtype) VALUES (%s,%s,%s)", record)
        db.commit()
    except pymysql.Error:
        logger.error("Could not add error message to message database")
        db_connect()


# Function to count records in a whatever table specified
def count_rows(tbl_name):
    try:
        query = """SELECT COUNT(*) FROM %s"""
        query = query % tbl_name
        cursor.execute(query)
        row = cursor.fetchone()
        num_rows = row[0]
    except pymysql.Error:
        num_rows = 0
        logger.error('unable to count records from table ' + tbl_name)
        log_error('unable to count records from the table ' + tbl_name)
    return num_rows


# Function to prune the database to prevent it from gowing too large
def prune_database(tbl_name, max_records):
    number_of_rows = count_rows(tbl_name)
    rows_to_remove = number_of_rows - max_records
    record = [rows_to_remove]
    if rows_to_remove > 0:
        query = """DELETE FROM %s ORDER BY idx LIMIT %%s"""
        query = query % tbl_name
        cursor.execute(query, record)
    else:
        log_info("Max records not reached in table " + tbl_name + ": no records removed")


# Function to add a new record to the database with current time, temp and pressure
def add_new_data_record(temp, press, hum):
    cur_time = time.strftime('%Y-%m-%d %H:%M:%S')
    prune_database('data', 20158)
    try:
        record = [temp, cur_time, press, hum]
        cursor.execute("""INSERT INTO data (cels,readdatetime,bar,humidity) VALUES (%s,%s,%s,%s)""", record)
        db.commit()
        log_info('Adding new data record')

    except pymysql.Error:
        log_error("Error occurred while adding new data record")
        db_connect()


# Function to add a new record to the pressure table
def add_new_pressure_record(press):
    cur_time = time.strftime('%Y-%m-%d %H:%M:%S')
    prune_database('pressure', 29)
    try:
        record = [cur_time, press]
        cursor.execute("""INSERT INTO pressure (datetime,psi) VALUES (%s,%s)""", record)
        db.commit()
        log_info('Adding new pressure record')

    except pymysql.Error:
        log_error("Error occurred while adding new pressure record")
        db_connect()


# Attempt to re-connect to the database
def db_connect():
    try:
        global db
        global cursor
        db = pymysql.connect(ip_address, user_name, password, database_name)
        cursor = db.cursor()
        cursor.execute("SELECT VERSION()")
        db_results = cursor.fetchone()
        log_info("Database connection established")
        log_info(str(db_results))
    except pymysql.Error:
        logger.error("Database reconnection failed, will retry in 5 seconds")
        print("Database reconnection failed, will retry in 5 seconds")


def send_sms(sms_msg):
    sms_client = Client(account_sid, auth_token)
    sms_message = sms_client.messages.create(
        body=sms_msg,
        from_=from_num,
        to=phone_num
    )
    print(sms_message.sid)


# send a quick sms to show that the system is working
# send_sms("Well Status program activated.  Water pressure is being monitored.")
# Connect to the database
try:
    db = pymysql.connect(ip_address, user_name, password, database_name)
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    results = cursor.fetchone()
    log_info("Program started")
    log_info(str(results))
except pymysql.Error:
    logger.error('Initial database connection failure on startup, exiting...')
    print("Initial database connection failure on startup, program exited...")
    sys.exit()


# Main loop.
while True:
    count += 1
    try:
        data = ser.readline()
        dataString = data.decode('utf8')
    except serial.SerialException:
        dataString = "x:0:x:0:X:0"
    except AttributeError:
        dataString = "x:0:x:0:x:0"
    if dataString:
        log_info(dataString)
        print(dataString)
        dataArray = dataString.split(":")
        try:
            tmpVal = float(dataArray[3])
            if math.isnan(tmpVal):
                log_error("Invalid temp argument, extracted value is NaN")
                print("Invalid temp argument")
            else:
                tempC = tmpVal
            tmpVal = float(dataArray[1])
            if math.isnan(tmpVal):
                log_error("Invalid pressure argument, extracted value is Nan")
                print("Invalid pressure argument")
            else:
                pressRead = tmpVal
            tmpVal = float(dataArray[5])
            if math.isnan(tmpVal):
                log_error("Invalid humidity argument, extracted value is Nan")
                print("Invalid humidity argument")
            else:
                humidity = tmpVal
        except math:
            log_error("Bad data from arduino")
            print("Bad data from arduino")

# convert the celcius temp value into a temperature fahrenheit
    temperature = convert_temp(tempC, 2)
    pressure = convert_press(pressRead, 2)
    print("Temperature: "+str(temperature)+" - Pressure: "+str(pressure)+" - Humidity: "+str(humidity))

# prune the message table down to 100(000) table if necessary to keep it from growing too big
    prune_database('messages', 4999)

# add a new record to the pressure table
    add_new_pressure_record(pressure)

# add a new record to the database every 30th read

    if newStart:
        msg = "Well status program activated, water pressure is being monitored.  Initial pressure:"+str(pressure)+"."
        send_sms(msg)
        newStart = False

    if count % 60 == 0:
        add_new_data_record(temperature, pressure, humidity)
        count = 0
# monitor the keyboard during the delay to break on ctrl-c
    try:
        time.sleep(5)

    except KeyboardInterrupt:
        log_info("Program exited")
        db.close()
        sys.exit()
