from add_to_saf import add_to_download_saf
from downloader import downloader
from multiprocessing import Process
import time

if __name__ == "__main__":
    while(True):
        p1 = Process(target=add_to_download_saf)
        p2 = Process(target=downloader)

        p1.start()
        p2.start()
        
        p1.join()
        p2.join()

        time.sleep(1)
