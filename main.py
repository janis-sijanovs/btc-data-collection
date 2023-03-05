import csv
import json
import websocket
import datetime

PAIR = "btcbusd"
SOCKET = f"wss://stream.binance.com:9443/ws/{PAIR.lower()}@trade"

MAX_ROWS = 999999

row_count = 0
file_count = 1
date = datetime.date.today()


def on_open(conn_id):
    print("Connected!")


def on_error(conn_id, error):
    print(f"Error: {error}")
    if "ermission" in str(error):
        global file_count
        global row_count
        file_count += 1
        row_count = 0


def on_message(ws, message):
    global row_count
    global file_count
    global date

    data = json.loads(message)
    price = float(data['p'])

    if datetime.date.today() != date:
        date = datetime.date.today()
        row_count = 0
        file_count = 0

    if row_count == MAX_ROWS:
        file_count += 1
        row_count = 0

    row_count += 1

    with open(f'{datetime.date.today()}-{file_count}.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([datetime.datetime.now().strftime("%H:%M:%S"), price])

ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_message=on_message, on_error=on_error)
ws.run_forever()

if input() == "e":
    ws.close()