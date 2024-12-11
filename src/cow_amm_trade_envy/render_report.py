import os
import shutil

command = ("uv run quarto render trade_envy_report.ipynb "
           "--output trade_envy_report.html --embed-resources")
os.system(command)

os.makedirs("outputs", exist_ok=True)
shutil.move("trade_envy_report.html", "outputs/trade_envy_report.html")
