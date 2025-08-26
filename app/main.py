
import os, sys, logging, threading
import PySimpleGUI as sg
from analyzer import ClientConfig, AvitoClient, load_items_from_excel, process_items, save_output

def resource_path(rel):
    base = getattr(sys, "_MEIPASS", os.path.dirname(__file__))
    return os.path.join(base, rel)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
sg.theme("SystemDefault")

def create_layout():
    return [[sg.Column([
        [sg.Text("Excel файл:"), sg.Input(key="-EXCEL-", expand_x=True, enable_events=True), sg.FileBrowse(file_types=(("Excel","*.xlsx;*.xls"),))],
        [sg.Text("Cookies (txt):"), sg.Input(key="-COOK-", expand_x=True), sg.FileBrowse(file_types=(("Cookies","*.txt"),))],
        [sg.Text("Запросов в минуту:"), sg.Spin(values=list(range(4,61)), initial_value=12, key="-RATE-", size=(5,1)), sg.Text("Burst:"), sg.Spin(values=list(range(1,11)), initial_value=3, key="-BURST-", size=(5,1))],
        [sg.ProgressBar(100, orientation="h", size=(40,20), key="-PROG-")],
        [sg.Multiline(size=(80,12), key="-LOG-", autoscroll=True, write_only=True, disabled=True)],
        [sg.Button("Старт", key="-START-", button_color=("white","#2e7d32")), sg.Button("Стоп", key="-STOP-", button_color=("white","#c62828"), disabled=True), sg.Button("Выход", key="-EXIT-")],
        [sg.Text("Перетащи файл Excel сюда:"), sg.FileDrop(key="-DROP-", enable_events=True, drag_and_drop=True)]
    ], expand_x=True)]]

def main():
    icon_path = resource_path(os.path.join("assets","icon.ico"))
    window = sg.Window("Avito Price Analyzer (Compliant)", create_layout(), icon=icon_path, finalize=True)
    stop_event = threading.Event(); worker=None

    def ui_log(m): window["-LOG-"].update(m+"\n", append=True)
    def on_progress(done,total,note): window["-PROG-"].update(int((done/total)*100) if total else 0); ui_log(f"[{done}/{total}] {note}")

    def run(excel,cookies,rate,burst):
        try:
            cfg=ClientConfig(rate_per_min=rate, burst=burst); client=AvitoClient(cookies,cfg)
            items=load_items_from_excel(excel); ui_log(f"Загружено позиций: {len(items)}")
            res=process_items(items, client, checkpoint="checkpoint.csv", stop_event=stop_event, progress_cb=on_progress)
            csv,xlsx=save_output(res, excel); ui_log(f"Готово. CSV: {csv} | XLSX: {xlsx}")
        except Exception as e: ui_log(f"Ошибка: {e}")
        finally: window["-START-"].update(disabled=False); window["-STOP-"].update(disabled=True)

    while True:
        ev, val = window.read(timeout=100)
        if ev in (sg.WIN_CLOSED, "-EXIT-"):
            if worker and worker.is_alive(): stop_event.set(); worker.join(timeout=2)
            break
        if ev == "-DROP-": window["-EXCEL-"].update(val["-DROP-"])
        if ev == "-START-":
            excel=val["-EXCEL-"]; cookies=val["-COOK-"] or None; rate=int(val["-RATE-"]); burst=int(val["-BURST-"])
            if not excel or not os.path.exists(excel): sg.popup_error("Укажи корректный Excel файл"); continue
            stop_event.clear(); window["-START-"].update(disabled=True); window["-STOP-"].update(disabled=False); window["-LOG-"].update(""); window["-PROG-"].update(0)
            worker=threading.Thread(target=run, args=(excel,cookies,rate,burst), daemon=True); worker.start()
        if ev == "-STOP-":
            if worker and worker.is_alive(): stop_event.set(); ui_log("Остановка...")
            window["-STOP-"].update(disabled=True)
    window.close()

if __name__ == "__main__":
    main()
