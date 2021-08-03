import schedule
import time
from migrasi_source import main
from transform import transform

def etljob():
    main()
    transform()

schedule.every(1).minutes.do(etljob)

while True:
    schedule.run_pending()
    time.sleep(1)