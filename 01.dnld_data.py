import cdsapi
import os
import threading
from queue import Queue
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

# Dataset and common request parameters
dataset = "reanalysis-era5-single-levels"
base_request = {
    "product_type": ["reanalysis"],
    "variable": [
        "2m_dewpoint_temperature",
        "2m_temperature"
    ],
    "day": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31"
    ],
    "time": [
        "00:00", "01:00", "02:00",
        "03:00", "04:00", "05:00",
        "06:00", "07:00", "08:00",
        "09:00", "10:00", "11:00",
        "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00",
        "18:00", "19:00", "20:00",
        "21:00", "22:00", "23:00"
    ],
    "data_format": "netcdf",
    "area": [90, -180, 0, 180]  # North, West, South, East
}

# Define download directory
download_dir = "./ERA5_downloads"

print(f"Checking if download directory {download_dir} exists...")
if not os.path.exists(download_dir):
    print(f"Directory {download_dir} does not exist. Creating directory...")
    os.makedirs(download_dir)
else:
    print(f"Directory {download_dir} already exists.")

# Initialize the CDS API client
client = cdsapi.Client()

# Define years and months
years = [
    "1981", "1982", "1983", "1984", "1985", "1986",
    "1987", "1988", "1989", "1990", "1991", "1992",
    "1993", "1994", "1995", "1996", "1997", "1998",
    "1999", "2000", "2001", "2002", "2003", "2004",
    "2005", "2006", "2007", "2008", "2009", "2010"
]

months = [
    "01", "02", "03", "04", "05", "06",
    "07", "08", "09", "10", "11", "12"
]

# Define download task queue
queue = Queue()

# Function to download data
def download_era5_data(year, month, download_dir):
    request = base_request.copy()
    request["year"] = [year]
    request["month"] = [month]
    filename = f"era5_{year}_{month}.nc"
    filepath = os.path.join(download_dir, filename)

    if os.path.exists(filepath):
        print(f"File {filename} already exists. Skipping download.")
    else:
        print(f"Downloading data for {year}-{month}...")
        client.retrieve(dataset, request).download(filepath)
        print(f"Download completed: {filename}")

# Worker thread class
class DownloadWorker(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            year, month = self.queue.get()
            try:
                download_era5_data(year, month, download_dir)
            except Exception as e:
                print(f"Error downloading data for {year}-{month}: {e}")
            finally:
                self.queue.task_done()

# Create worker threads
print("Creating worker threads...")
for i in range(4):
    worker = DownloadWorker(queue)
    worker.daemon = True
    worker.start()
    print(f"Worker thread {worker.name} started.")

# Add tasks to queue
print("Adding download tasks to the queue...")
for year in years:
    for month in months:
        queue.put((year, month))

# Wait for all tasks to complete
print("Waiting for all tasks to complete...")
queue.join()
print("All download tasks completed.")
