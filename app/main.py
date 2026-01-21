import asyncio
import json
import os
from datetime import datetime

import aiohttp
import pandas as pd
from celery.result import AsyncResult
from fastapi.staticfiles import StaticFiles
from nicegui import app, ui

from app.database import get_columns, get_date_columns, get_db_engine, get_tables
from app.parser_engine import ParserEngine
from app.preset_manager import PresetManager
from app.worker import cancel_task, run_etl_job

os.makedirs("output", exist_ok=True)
job_history = []
current_preset_id = None
current_task_id = None


# --- ASSETS & STYLES ---
def init_styles():
    ui.add_head_html(
        """
        <link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;700&family=JetBrains+Mono:wght@400;800&display=swap" rel="stylesheet">
        <style>
            :root {
                --neon-blue: #00f3ff;
                --neon-purple: #bc13fe;
                --glass-bg: rgba(15, 23, 42, 0.6);
                --glass-border: rgba(255, 255, 255, 0.1);
            }
            body { 
                font-family: 'Rajdhani', sans-serif; 
                background-color: #050505;
                color: #e2e8f0;
                overflow-x: hidden;
            }
            .bg-animation {
                position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; z-index: -1;
                background: 
                    radial-gradient(circle at 15% 50%, rgba(76, 29, 149, 0.15), transparent 25%), 
                    radial-gradient(circle at 85% 30%, rgba(56, 189, 248, 0.15), transparent 25%);
                animation: pulse-bg 10s infinite alternate;
            }
            @keyframes pulse-bg { 0% { opacity: 0.8; } 100% { opacity: 1; } }
            .glass-card {
                background: var(--glass-bg);
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                border: 1px solid var(--glass-border);
                border-radius: 16px;
                box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
                transition: all 0.3s ease;
            }
            .glass-card:hover {
                box-shadow: 0 0 15px rgba(0, 243, 255, 0.1);
                border-color: rgba(0, 243, 255, 0.3);
            }
            .neon-text { text-shadow: 0 0 10px rgba(0, 243, 255, 0.5); letter-spacing: 2px; }
            .q-field__native { color: white !important; font-family: 'JetBrains Mono'; }
            .q-field__label { color: #94a3b8 !important; }
            .q-field__control:before { border-color: #334155 !important; }
            .task-dock { 
                position: fixed; bottom: 30px; right: 30px; width: 420px; z-index: 9999; 
                background: #0f172a; border: 1px solid #1e293b; border-radius: 12px;
                box-shadow: 0 20px 50px rgba(0,0,0,0.5);
                transition: transform 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            }
            .task-dock.hidden { transform: translateY(200%); }
            .task-dock.minimized { transform: translateY(85%); }
            .dock-header { cursor: pointer; }
            .terminal-window { 
                background-color: #0a0a0a; color: #00f3ff; 
                font-family: 'JetBrains Mono', monospace; 
                border: 1px solid #333;
                border-left: 3px solid var(--neon-purple);
            }
            ::-webkit-scrollbar { width: 8px; }
            ::-webkit-scrollbar-track { background: #0f172a; }
            ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
            ::-webkit-scrollbar-thumb:hover { background: #475569; }
        </style>
    """
    )


def add_to_history(filename, rows, status="Success"):
    timestamp = datetime.now().strftime("%H:%M")
    job_history.insert(
        0,
        {
            "time": timestamp,
            "file": filename,
            "rows": rows,
            "status": status,
            "link": f"/output/{filename}",
        },
    )
    if len(job_history) > 8:
        job_history.pop()


def init_ui():
    init_styles()
    ui.element("div").classes("bg-animation")

    # --- HEADER ---
    with ui.header().classes(
        "bg-transparent h-20 flex items-center px-8 border-b border-white/10"
    ):
        with ui.row().classes("items-center gap-4"):
            ui.icon("hub", size="36px").classes("text-cyan-400 drop-shadow-lg")
            with ui.column().classes("gap-0"):
                ui.label("UNIVERSAL ETL").classes(
                    "text-2xl font-black text-white neon-text leading-none"
                )
                ui.label("v21.0 • ENTERPRISE EDITION").classes(
                    "text-xs text-purple-400 font-mono tracking-widest"
                )
        ui.space()
        ui.button(
            icon="settings",
            on_click=lambda: ui.notify("Settings Locked", type="warning"),
        ).props("flat round color=grey")

    # --- DOCK ---
    with ui.card().classes("task-dock hidden") as job_dock:
        with ui.row().classes(
            "dock-header w-full justify-between items-center bg-slate-900 p-3 border-b border-slate-800"
        ):
            with ui.row().classes("items-center gap-3"):
                dock_status_icon = ui.icon("radio_button_checked", size="xs").classes(
                    "text-cyan-400 animate-pulse"
                )
                dock_title = ui.label("SYSTEM IDLE").classes(
                    "text-xs font-bold text-cyan-400 font-mono tracking-widest"
                )
            with ui.row().classes("gap-1"):
                ui.button(
                    icon="keyboard_arrow_down",
                    on_click=lambda: job_dock.classes(add="minimized"),
                ).props("flat round dense size=sm color=grey")
                ui.button(
                    icon="keyboard_arrow_up",
                    on_click=lambda: job_dock.classes(remove="minimized"),
                ).props("flat round dense size=sm color=grey")

                def close_dock():
                    if current_task_id:
                        ui.notify("PROCESS ACTIVE. CANNOT CLOSE.", type="negative")
                    else:
                        job_dock.classes(add="hidden")

                ui.button(icon="close", on_click=close_dock).props(
                    "flat round dense size=sm color=red"
                )

        with ui.column().classes("p-4 w-full gap-3 bg-slate-900/90"):
            dock_progress = (
                ui.linear_progress(value=0)
                .classes("h-1")
                .props("track-color=grey-9 color=cyan-4")
            )
            dock_log = ui.log(max_lines=100).classes(
                "terminal-window w-full h-32 text-xs p-2 rounded"
            )
            with ui.row().classes("w-full gap-2 pt-2"):

                async def stop_job():
                    global current_task_id
                    if current_task_id:
                        cancel_task(current_task_id)
                        dock_log.push("[SYSTEM] KILL SIGNAL SENT.")
                        dock_status_icon.classes(
                            remove="animate-pulse text-cyan-400", add="text-red-500"
                        )
                        dock_title.text = "TERMINATED"
                        current_task_id = None

                ui.button("EMERGENCY STOP", on_click=stop_job).props(
                    "flat dense color=red icon=gpp_bad"
                ).classes("flex-grow border border-red-900/50")
                dock_download = ui.button("RETRIEVE DATA", icon="download").classes(
                    "bg-emerald-600 text-white flex-grow hidden shadow-[0_0_15px_rgba(16,185,129,0.4)]"
                )

    # --- SAVE DIALOG ---
    with ui.dialog() as save_dialog, ui.card().classes("glass-card w-96 p-6"):
        ui.label("ENCRYPT CONFIGURATION").classes(
            "text-lg font-bold text-white mb-4 tracking-widest"
        )
        p_name = (
            ui.input("Protocol Name")
            .props("dark outlined dense")
            .classes("w-full mb-2")
        )
        p_desc = (
            ui.input("Description").props("dark outlined dense").classes("w-full mb-4")
        )

        def commit_save():
            if not p_name.value:
                return ui.notify("Name Required", type="warning")
            config_dump = {
                "host": host.value,
                "user": user.value,
                "password": password.value,
                "dbname": dbname.value,
                "use_join": mode_group.value,
                "primary_table": p_table_sel.value,
                "primary_col": p_col_sel.value,
                "secondary_table": s_table_sel.value,
                "secondary_col": s_col_sel.value,
                "single_table": single_table_sel.value,
                "single_id": single_id_sel.value,
                "target_col": json_col_sel.value,
                "filter_col": filter_col_sel.value,
                "filter_val": filter_val.value,
                "date_col": date_col_sel.value,
                "is_url_mode": is_s3.value,
                "base_url": base_url.value,
                "pan_col": pan_col_sel.value,
                "mobile_col": mobile_col_sel.value,
            }
            PresetManager.save_preset(p_name.value, p_desc.value, config_dump)
            ui.notify("Configuration Saved.", type="positive", icon="save")
            preset_list.refresh()
            save_dialog.close()

        ui.button("SAVE PROTOCOL", on_click=commit_save).classes(
            "w-full bg-purple-600 text-white font-bold"
        )

    # --- MAIN LAYOUT ---
    with ui.row().classes("w-full max-w-[1600px] mx-auto p-8 gap-8"):

        # LEFT COLUMN
        with ui.column().classes("w-2/3 gap-6"):
            with ui.card().classes("glass-card w-full p-0 overflow-hidden"):
                with ui.tabs().classes("w-full text-slate-400 bg-black/20") as tabs:
                    tab_saved = ui.tab("PRESETS", icon="bookmark_border")
                    tab_new = ui.tab("CONNECTION", icon="dns")

                with ui.tab_panels(tabs, value=tab_saved).classes(
                    "w-full p-6 bg-transparent animated fadeIn"
                ):
                    with ui.tab_panel(tab_saved).classes(
                        "p-0 gap-3 flex flex-col h-64 overflow-y-auto pr-2"
                    ):

                        @ui.refreshable
                        def preset_list():
                            presets = PresetManager.load_presets()
                            if not presets:
                                ui.label("NO DATA PROTOCOLS FOUND.").classes(
                                    "text-sm text-gray-500 font-mono w-full text-center mt-10"
                                )
                                return
                            for pid, p in presets.items():
                                with ui.row().classes(
                                    "w-full items-center border border-white/5 p-3 rounded-lg hover:bg-white/5 transition-colors cursor-pointer group"
                                ):
                                    with ui.column().classes("gap-0 flex-grow"):
                                        ui.label(p["name"]).classes(
                                            "font-bold text-cyan-400 group-hover:text-white transition-colors"
                                        )
                                        ui.label(p["description"]).classes(
                                            "text-xs text-gray-500 font-mono"
                                        )

                                    async def load_this(pid=pid, p=p):
                                        global current_preset_id
                                        current_preset_id = pid
                                        ui.notify(
                                            f"Initializing Protocol: {p['name']}...",
                                            type="info",
                                            color="black",
                                        )
                                        conf = p["config"]
                                        host.value = conf.get("host")
                                        user.value = conf.get("user")
                                        password.value = conf.get("password")
                                        dbname.value = conf.get("dbname")
                                        mode_group.value = conf.get("use_join")
                                        try:
                                            db_conf = {
                                                "host": host.value,
                                                "user": user.value,
                                                "password": password.value,
                                                "dbname": dbname.value,
                                            }
                                            eng = get_db_engine(db_conf)
                                            tables = get_tables(eng)
                                            p_table_sel.options = tables
                                            s_table_sel.options = tables
                                            single_table_sel.options = tables
                                            p_table_sel.update()
                                            s_table_sel.update()
                                            single_table_sel.update()
                                            tabs.set_value(tab_new)
                                        except Exception as e:
                                            return ui.notify(
                                                f"Link Failure: {e}", type="negative"
                                            )

                                        if conf.get("use_join"):
                                            pt = conf.get("primary_table")
                                            st = conf.get("secondary_table")
                                            if pt in tables:
                                                p_table_sel.value = pt
                                                p_table_sel.update()
                                                cols = get_columns(eng, pt)
                                                p_col_sel.options = cols
                                                p_col_sel.update()
                                                pan_col_sel.options = cols
                                                pan_col_sel.update()
                                                mobile_col_sel.options = cols
                                                mobile_col_sel.update()

                                                p_col_sel.value = conf.get(
                                                    "primary_col"
                                                )
                                                p_col_sel.update()
                                                pan_col_sel.value = conf.get("pan_col")
                                                pan_col_sel.update()
                                                mobile_col_sel.value = conf.get(
                                                    "mobile_col"
                                                )
                                                mobile_col_sel.update()

                                            if st in tables:
                                                s_table_sel.value = st
                                                s_table_sel.update()
                                                cols = get_columns(eng, st)
                                                s_col_sel.options = cols
                                                json_col_sel.options = cols
                                                filter_col_sel.options = cols
                                                s_col_sel.update()
                                                json_col_sel.update()
                                                filter_col_sel.update()
                                                s_col_sel.value = conf.get(
                                                    "secondary_col"
                                                )
                                                s_col_sel.update()
                                        else:
                                            st = conf.get("single_table")
                                            if st in tables:
                                                single_table_sel.value = st
                                                single_table_sel.update()
                                                cols = get_columns(eng, st)
                                                single_id_sel.options = cols
                                                json_col_sel.options = cols
                                                filter_col_sel.options = cols
                                                single_id_sel.update()
                                                json_col_sel.update()
                                                filter_col_sel.update()
                                                single_id_sel.value = conf.get(
                                                    "single_id"
                                                )
                                                single_id_sel.update()

                                        json_col_sel.value = conf.get("target_col")
                                        filter_col_sel.value = conf.get("filter_col")
                                        filter_val.value = conf.get("filter_val")
                                        is_s3.value = conf.get("is_url_mode")
                                        base_url.value = conf.get("base_url")
                                        dtbl = (
                                            conf.get("secondary_table")
                                            if conf.get("use_join")
                                            else conf.get("single_table")
                                        )
                                        if dtbl:
                                            date_col_sel.options = get_columns(
                                                eng, dtbl
                                            )
                                            date_col_sel.update()
                                            date_col_sel.value = conf.get("date_col")

                                        ui.notify(
                                            "SYSTEM READY.",
                                            type="positive",
                                            color="black",
                                            icon="check_circle",
                                        )

                                    ui.button(
                                        icon="play_arrow", on_click=load_this
                                    ).props("flat dense round color=cyan")
                                    ui.button(
                                        icon="delete",
                                        on_click=lambda pid=pid: [
                                            PresetManager.delete_preset(pid),
                                            preset_list.refresh(),
                                        ],
                                    ).props("flat dense round color=red")

                        preset_list()

                    with ui.tab_panel(tab_new).classes("p-0"):
                        with ui.column().classes("w-full gap-4"):
                            with ui.grid(columns=4).classes("w-full gap-4"):
                                host = (
                                    ui.input("Host")
                                    .props("dark outlined dense label-color=cyan")
                                    .classes("font-mono")
                                )
                                user = (
                                    ui.input("User")
                                    .props("dark outlined dense label-color=cyan")
                                    .classes("font-mono")
                                )
                                password = ui.input("Pass", password=True).props(
                                    "dark outlined dense label-color=cyan"
                                )
                                dbname = ui.input("Database").props(
                                    "dark outlined dense label-color=cyan"
                                )

                            async def load_schema():
                                try:
                                    ui.notify(
                                        "Establishing Uplink...",
                                        type="info",
                                        spinner=True,
                                        color="black",
                                    )
                                    db_conf = {
                                        "host": host.value,
                                        "user": user.value,
                                        "password": password.value,
                                        "dbname": dbname.value,
                                    }
                                    eng = get_db_engine(db_conf)
                                    tables = get_tables(eng)
                                    for d in [
                                        p_table_sel,
                                        s_table_sel,
                                        single_table_sel,
                                    ]:
                                        d.options = tables
                                        d.update()
                                    ui.notify(
                                        f"Uplink Established. {len(tables)} Nodes Found.",
                                        type="positive",
                                        color="green",
                                    )
                                    step2.classes(
                                        remove="opacity-30 pointer-events-none grayscale"
                                    )
                                except Exception as e:
                                    ui.notify(f"Uplink Failed: {e}", type="negative")

                            ui.button("ESTABLISH UPLINK", on_click=load_schema).classes(
                                "w-full bg-cyan-900/50 text-cyan-400 border border-cyan-500/50 hover:bg-cyan-500/20 font-bold tracking-widest"
                            )

            with ui.card().classes(
                "glass-card w-full p-6 opacity-30 pointer-events-none grayscale transition-all duration-700"
            ) as step2:
                with ui.row().classes("justify-between items-center mb-6"):
                    ui.label("QUERY MATRIX").classes(
                        "text-sm font-bold text-slate-500 tracking-[4px]"
                    )
                    mode_group = ui.toggle(
                        {False: "SINGLE NODE", True: "JOINED NODES"}, value=True
                    ).props(
                        "no-caps push color=purple-9 text-color=white toggle-color=purple-6"
                    )

                async def on_table_select(e, target_col_dropdowns=[]):
                    if not e.value:
                        return
                    try:
                        db_conf = {
                            "host": host.value,
                            "user": user.value,
                            "password": password.value,
                            "dbname": dbname.value,
                        }
                        eng = get_db_engine(db_conf)
                        cols = get_columns(eng, e.value)
                        for d in target_col_dropdowns:
                            d.options = cols
                            d.value = None
                            d.update()
                        if get_date_columns(eng, e.value):
                            date_col_sel.options = cols
                            date_col_sel.update()
                    except:
                        pass

                # --- JOIN MODE WITH PAN/MOBILE ---
                with ui.grid(columns=2).bind_visibility_from(
                    mode_group, "value", value=True
                ).classes("w-full gap-4"):
                    p_table_sel = ui.select(
                        [], label="Primary Table", with_input=True
                    ).props("dark outlined dense options-dense behavior=menu")
                    p_col_sel = ui.select(
                        [], label="Primary Key", with_input=True
                    ).props("dark outlined dense options-dense behavior=menu")

                    # New Columns
                    pan_col_sel = ui.select(
                        [], label="DB PAN Column (Opt)", with_input=True
                    ).props(
                        "dark outlined dense options-dense behavior=menu label-color=orange"
                    )
                    mobile_col_sel = ui.select(
                        [], label="DB Mobile Column (Opt)", with_input=True
                    ).props(
                        "dark outlined dense options-dense behavior=menu label-color=orange"
                    )

                    p_table_sel.on_value_change(
                        lambda e: on_table_select(
                            e, [p_col_sel, pan_col_sel, mobile_col_sel]
                        )
                    )

                    s_table_sel = ui.select(
                        [], label="JSON Table", with_input=True
                    ).props("dark outlined dense options-dense behavior=menu")
                    s_col_sel = ui.select(
                        [], label="Foreign Key", with_input=True
                    ).props("dark outlined dense options-dense behavior=menu")
                    s_table_sel.on_value_change(
                        lambda e: on_table_select(
                            e, [s_col_sel, json_col_sel, filter_col_sel]
                        )
                    )

                with ui.grid(columns=2).bind_visibility_from(
                    mode_group, "value", value=False
                ).classes("w-full gap-4"):
                    single_table_sel = ui.select(
                        [], label="Target Table", with_input=True
                    ).props("dark outlined dense options-dense behavior=menu")
                    single_id_sel = ui.select(
                        [], label="ID Column", with_input=True
                    ).props("dark outlined dense options-dense behavior=menu")
                    single_table_sel.on_value_change(
                        lambda e: on_table_select(
                            e, [single_id_sel, json_col_sel, filter_col_sel]
                        )
                    )

                ui.separator().classes("my-6 opacity-20")
                with ui.grid(columns=3).classes("w-full gap-4"):
                    json_col_sel = ui.select(
                        [], label="Target JSON Column", with_input=True
                    ).props("dark outlined dense label-color=green")
                    filter_col_sel = ui.select(
                        [], label="Filter Column", with_input=True
                    ).props("dark outlined dense")
                    filter_val = ui.input("Filter Value").props("dark outlined dense")

                with ui.expansion("ADVANCED PROTOCOLS", icon="code").classes(
                    "w-full mt-4 text-slate-400"
                ):
                    with ui.column().classes("p-4 w-full bg-black/20 rounded"):
                        is_s3 = ui.switch("Remote JSON Fetch (S3/URL)").props(
                            "color=cyan"
                        )
                        base_url = (
                            ui.input("Base URL Endpoint")
                            .props('dark outlined dense placeholder="https://..."')
                            .classes("w-full font-mono")
                            .bind_visibility_from(is_s3, "value")
                        )

            with ui.card().classes(
                "glass-card w-full p-6 border-l-4 border-l-purple-500"
            ):
                ui.label("TEMPORAL SCOPE").classes(
                    "text-sm font-bold text-purple-400 tracking-[4px] mb-4"
                )
                with ui.grid(columns=3).classes("w-full gap-4"):
                    date_col_sel = ui.select(
                        [], label="Timestamp Column", with_input=True
                    ).props("dark outlined dense behavior=menu w-full")
                    start_date = ui.input("Start Horizon").props(
                        "dark outlined dense type=date"
                    )
                    end_date = ui.input("End Horizon").props(
                        "dark outlined dense type=date"
                    )

        # RIGHT COLUMN
        with ui.column().classes("w-1/3 gap-6"):
            with ui.card().classes("glass-card w-full p-6 flex flex-col gap-4"):
                ui.label("COMMAND CENTER").classes(
                    "text-sm font-bold text-slate-500 tracking-[4px]"
                )

                async def run_test():
                    try:
                        ui.notify("Probing Database...", type="info", color="black")
                        db_conf = {
                            "host": host.value,
                            "user": user.value,
                            "password": password.value,
                            "dbname": dbname.value,
                        }
                        eng = get_db_engine(db_conf)
                        tbl = (
                            s_table_sel.value
                            if mode_group.value
                            else single_table_sel.value
                        )
                        col = json_col_sel.value or "*"
                        q = f"SELECT {col} FROM {tbl}"
                        conds = []
                        if filter_col_sel.value and filter_val.value:
                            conds.append(f"{filter_col_sel.value}='{filter_val.value}'")
                        if date_col_sel.value and start_date.value and end_date.value:
                            conds.append(
                                f"{date_col_sel.value} BETWEEN '{start_date.value}' AND '{end_date.value}'"
                            )
                        if conds:
                            q += " WHERE " + " AND ".join(conds)
                        q += " LIMIT 1"
                        df = pd.read_sql(q, eng)
                        if not df.empty:
                            raw = df.iloc[0, 0]
                            if is_s3.value:
                                url = (base_url.value or "") + str(raw)
                                try:
                                    async with aiohttp.ClientSession() as session:
                                        async with session.get(
                                            url, timeout=5, ssl=False
                                        ) as resp:
                                            if resp.status == 200:
                                                raw = await resp.json()
                                            else:
                                                ui.notify(
                                                    f"Remote Fetch Failed: {resp.status}",
                                                    type="warning",
                                                )
                                                return
                                except:
                                    ui.notify("Network Error", type="negative")
                                    return
                            fmt = ParserEngine.identify_format(raw)
                            ui.notify(
                                f"DATA SIGNATURE: {fmt}",
                                type="positive",
                                color="purple",
                                icon="fingerprint",
                            )
                            btn_run.enable()
                            btn_save.enable()
                            if current_preset_id:
                                btn_update.enable()
                                btn_update.text = f"UPDATE PROTOCOL"
                        else:
                            ui.notify("Query returned ZERO VOID.", type="warning")
                    except Exception as e:
                        ui.notify(str(e), type="negative")

                ui.button("VERIFY INTEGRITY", on_click=run_test).classes(
                    "w-full bg-slate-800 text-slate-300 font-bold border border-slate-700 hover:bg-slate-700"
                )

                async def start_job():
                    job_dock.classes(remove="hidden minimized")
                    dock_download.classes(add="hidden")
                    dock_status_icon.name = "radio_button_checked"
                    dock_status_icon.classes(
                        add="animate-pulse text-cyan-400", remove="text-red-500"
                    )
                    dock_title.text = "INITIALIZING ENGINE..."
                    dock_progress.set_value(0)
                    dock_log.clear()
                    dock_log.push("[SYSTEM] ENGINE START SEQUENCE...")
                    db_conf = {
                        "host": host.value,
                        "user": user.value,
                        "password": password.value,
                        "dbname": dbname.value,
                    }
                    q_conf = {
                        "use_join": mode_group.value,
                        "primary_table": p_table_sel.value,
                        "primary_col": p_col_sel.value,
                        "secondary_table": s_table_sel.value,
                        "secondary_col": s_col_sel.value,
                        "single_table": single_table_sel.value,
                        "single_id": single_id_sel.value,
                        "target_col": json_col_sel.value,
                        "filter_col": filter_col_sel.value,
                        "filter_val": filter_val.value,
                        "date_col": date_col_sel.value,
                        "start_date": start_date.value,
                        "end_date": end_date.value,
                        "is_url_mode": is_s3.value,
                        "base_url": base_url.value,
                        "pan_col": pan_col_sel.value,
                        "mobile_col": mobile_col_sel.value,  # PASS NEW COLS
                    }
                    global current_task_id
                    task = run_etl_job.delay(db_conf, q_conf)
                    current_task_id = task.id
                    last_st = ""
                    try:
                        while True:
                            if not current_task_id:
                                break
                            await asyncio.sleep(0.5)
                            res = AsyncResult(task.id)
                            if res.state == "PROGRESS":
                                st = res.info.get("status", "")
                                if st != last_st:
                                    dock_log.push(f"> {st}")
                                    last_st = st
                                    dock_title.text = "PROCESSING..."
                                dock_progress.set_value(
                                    res.info.get("progress", 0) / 100
                                )
                            elif res.state == "SUCCESS":
                                result = res.result
                                if (
                                    isinstance(result, dict)
                                    and result.get("status") == "Completed"
                                ):
                                    fetched = result.get("total_fetched", "?")
                                    valid = result.get("total_rows", 0)
                                    employees = result.get("employees_found", 0)

                                    dock_log.push(
                                        f"✅ COMPLETE. IN: {fetched} | VALID: {valid} | EMP: {employees}"
                                    )
                                    dock_title.text = "JOB COMPLETE"
                                    dock_status_icon.classes(
                                        remove="animate-pulse text-cyan-400",
                                        add="text-green-500",
                                    )
                                    dock_progress.set_value(1)

                                    # Safe File Path Access
                                    fp = result.get("file_path")
                                    if fp and os.path.exists(fp):
                                        dock_download.props(
                                            f"href=/output/{os.path.basename(fp)}"
                                        )
                                        dock_download.classes(remove="hidden")
                                        add_to_history(os.path.basename(fp), valid)
                                        history_list.refresh()

                                    current_task_id = None
                                    break
                                else:
                                    err = (
                                        result.get("error", "Unknown Error")
                                        if isinstance(result, dict)
                                        else str(result)
                                    )
                                    dock_log.push(f"❌ WORKER ERROR: {err}")
                                    dock_title.text = "FAILED"
                                    dock_status_icon.classes(
                                        remove="animate-pulse text-cyan-400",
                                        add="text-red-500",
                                    )
                                    current_task_id = None
                                    break
                            elif res.state == "FAILURE" or res.state == "REVOKED":
                                dock_log.push(f"❌ CRITICAL FAILURE: {str(res.result)}")
                                dock_title.text = "FAILED"
                                dock_status_icon.classes(
                                    remove="animate-pulse text-cyan-400",
                                    add="text-red-500",
                                )
                                current_task_id = None
                                break
                    except RuntimeError:
                        return

                btn_run = ui.button("INITIATE EXTRACTION", on_click=start_job).classes(
                    "w-full bg-gradient-to-r from-blue-600 to-cyan-600 hover:from-blue-500 hover:to-cyan-500 text-white font-black shadow-[0_0_20px_rgba(0,243,255,0.3)] h-12 text-lg tracking-widest"
                )
                btn_run.disable()

                with ui.row().classes("w-full gap-2"):

                    def update_current_preset():
                        if not current_preset_id:
                            return
                        config_dump = {
                            "host": host.value,
                            "user": user.value,
                            "password": password.value,
                            "dbname": dbname.value,
                            "use_join": mode_group.value,
                            "primary_table": p_table_sel.value,
                            "primary_col": p_col_sel.value,
                            "secondary_table": s_table_sel.value,
                            "secondary_col": s_col_sel.value,
                            "single_table": single_table_sel.value,
                            "single_id": single_id_sel.value,
                            "target_col": json_col_sel.value,
                            "filter_col": filter_col_sel.value,
                            "filter_val": filter_val.value,
                            "date_col": date_col_sel.value,
                            "is_url_mode": is_s3.value,
                            "base_url": base_url.value,
                            "pan_col": pan_col_sel.value,
                            "mobile_col": mobile_col_sel.value,
                        }
                        old_data = PresetManager.load_presets()[current_preset_id]
                        PresetManager.save_preset(
                            old_data["name"],
                            old_data["description"],
                            config_dump,
                            pid=current_preset_id,
                        )
                        ui.notify(
                            f"Protocol '{old_data['name']}' Updated",
                            type="positive",
                            color="black",
                        )
                        preset_list.refresh()

                    btn_update = ui.button(
                        "UPDATE", on_click=update_current_preset
                    ).classes(
                        "flex-grow bg-emerald-900/50 text-emerald-400 border border-emerald-700/50"
                    )
                    btn_update.disable()
                    btn_save = ui.button("SAVE NEW", on_click=save_dialog.open).classes(
                        "flex-grow bg-purple-900/50 text-purple-400 border border-purple-700/50"
                    )
                    btn_save.disable()

            with ui.card().classes("glass-card w-full p-6 flex-grow"):
                ui.label("DATA LOGS").classes(
                    "text-sm font-bold text-slate-500 tracking-[4px] mb-2"
                )

                @ui.refreshable
                def history_list():
                    for job in job_history:
                        with ui.row().classes(
                            "w-full justify-between items-center border-b border-white/5 py-3"
                        ):
                            with ui.column().classes("gap-0"):
                                ui.label(job["file"]).classes(
                                    "font-bold text-white text-xs truncate w-32"
                                )
                                ui.label(
                                    f"{job['rows']} records • {job['time']}"
                                ).classes("text-[10px] text-gray-500")
                            ui.button(
                                icon="download",
                                on_click=lambda l=job["link"]: ui.download(l),
                            ).props("flat dense round color=cyan size=sm")

                history_list()


init_ui()
app.mount("/output", StaticFiles(directory="output"), name="output")
ui.run(title="Universal ETL v21.0", reload=False, port=8080)
