import time
import subprocess

while True:
    subprocess.run(["python", "main.py"])
    time.sleep(86400)  