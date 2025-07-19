from datetime import datetime, timedelta
from time import sleep
import psycopg2
import math

import RPi.GPIO as GPIO
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
# GPIO.cleanup()
GPIO.setup(17, GPIO.IN)

READER_MODE = "FILTERED"
DEBUG = False


# The database send function
def send_db(record):
    dBconnection = None
    if DEBUG: print(datetime.now().strftime('%H:%M:%S'), "Sending to db", record)
    try:
        # Use .pg_service.conf for credentials
        dBconnection = psycopg2.connect(service="voda")
        dBc = dBconnection.cursor()
        dBc.execute("INSERT INTO voda ( time, waterl ) VALUES ( current_timestamp::timestamp(0),%s )", [record])
        dBconnection.commit()
        
    except Exception as e:
        print("psycopg2:", e)
        sleep(60)
        
    if dBconnection:
        dBc.close()
        dBconnection.close()
  

# Filtered reader to filter out glitches when optically reading the disc. Loop follows both stages
# of rotating water disc, i.e., one liter measurement. Both stages have to complete in order for
# one liter rotation to be actually complete. Stage 1 begins after a detected gpio state change.
# If state change is from high to low, i.e., True to False, start of the stage 1 is accepted and
# we'll move forward. Otherwise we'll never move forward.

def reader_filtered():
    delay_high = 1000 # Milliseconds. Stage 1, i.e., first rotation part of the disc has to at least this amount.
    delay_low = 100 # Milliseconds. Stage 0, i.e., second rotation part of the disc has to at least this amount.
    guard_period = 1000 # Milliseconds. Guard period between consecutive readings.
    db_frequency = 1 # Minutes. Database update frequency. Measurements are added.
    timer = 0
    liters = 0
    high_started = False
    low_started = False
    # Init guard period
    guard_time = datetime.now()
    # Init database last sent
    db_last = datetime.now() + timedelta(minutes=db_frequency) # Reset last database update
    # Init gpio state
    old_state = state = GPIO.input(17)

    while True:
        if datetime.now() >= guard_time: # Delay everything else if we're not out of the guard period yet
            # Get gpio state
            state = GPIO.input(17)

            if state == False and low_started == False:
                if DEBUG: print("Stage low started")
                low_started = True
                low_time = datetime.now()

            if old_state == False and state == True and low_started == True and high_started == False and datetime.now() >= low_time + timedelta(milliseconds=delay_low):
                if DEBUG: print("Stage high started")
                high_started = True
                high_time = datetime.now()

            if state == True and high_started == True and datetime.now() >= high_time + timedelta(milliseconds=delay_high):
                liters += 1
                if DEBUG: print("Both stages complete, liters", liters)
                high_started = low_started = False

            old_state = state # Reset state difference
            guard_time = datetime.now() + timedelta(milliseconds=guard_period) # Reset guard period

        # Database update follower. Sends updated reading every db_frequency. Converted minutes to 100's of milliseconds.
        if datetime.now() >= db_last:
            send_db(liters)
            liters = 0 # Reset liter counter and timer
            db_last = datetime.now() + timedelta(minutes=db_frequency) # Reset last database update

        sleep(0.1)


# Simple state change reader without any filtering - not used
def reader_simple():
    # Init gpio state
    old_state = state = GPIO.input(17)

    while True:
        state = GPIO.input(17)
        if state != old_state:
            if DEBUG: print("GPIO state change:", state)
            # Call db here...
        old_state = state
        sleep(0.1)


# Start main loop
if READER_MODE == "SIMPLE":
    reader_simple()
elif READER_MODE == "FILTERED":
    reader_filtered()
else:
    print ("Filter mode not defined, exiting...")


print(datetime.now().strftime('%-d. %H:%M:%S'), "Exited!")
