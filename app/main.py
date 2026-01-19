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


def add_to_history(filename, rows, status="Success"):
    timestamp = datetime.now().strftime("%H:%M:%S")
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
    if len(job_history) > 10:
        job_history.pop()


def init_ui():
    ui.add_head_html(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;800&display=swap');
            body { font-family: 'Inter', sans-serif; background-color: #f8fafc; }
            .nice-card { background: white; border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); border: 1px solid #e2e8f0; }
            .terminal-window { background-color: #0d1117; color: #58a6ff; font-family: 'JetBrains Mono', monospace; padding: 1rem; border-radius: 8px; height: 200px; overflow-y: auto; font-size: 0.8rem; }
            .task-dock { position: fixed; bottom: 20px; right: 20px; width: 400px; z-index: 9999; transition: all 0.3s ease; box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1); }
            .task-dock.minimized { width: 250px; bottom: 20px; height: 50px; overflow: hidden; }
            .dock-header { cursor: pointer; }
        </style>
    """
    )

    with ui.header().classes(
        "bg-slate-900 h-16 flex items-center px-6 border-b border-slate-700"
    ):
        ui.icon("hub", size="28px").classes("text-blue-500 mr-3")
        ui.label("UNIVERSAL ETL v18.1").classes(
            "text-lg font-black text-white tracking-widest"
        )

    with ui.card().classes(
        "task-dock hidden bg-white border border-slate-200"
    ) as job_dock:
        with ui.row().classes(
            "dock-header w-full justify-between items-center bg-slate-50 p-3 border-b"
        ):
            with ui.row().classes("items-center gap-2"):
                dock_status_icon = ui.icon("sync", size="xs").classes(
                    "text-blue-500 animate-spin"
                )
                dock_title = ui.label("Processing...").classes(
                    "text-sm font-bold text-slate-700"
                )
            with ui.row().classes("gap-1"):
                ui.button(
                    icon="remove", on_click=lambda: job_dock.classes(add="minimized")
                ).props("flat round dense size=sm")
                ui.button(
                    icon="open_in_full",
                    on_click=lambda: job_dock.classes(remove="minimized"),
                ).props("flat round dense size=sm")

                def close_dock():
                    if current_task_id:
                        ui.notify("Job running! Minimize instead.", type="warning")
                    else:
                        job_dock.classes(add="hidden")

                ui.button(icon="close", on_click=close_dock).props(
                    "flat round dense size=sm color=red"
                )

        with ui.column().classes("p-4 w-full gap-3"):
            dock_progress = (
                ui.linear_progress(value=0)
                .classes("h-2 rounded-full")
                .props("track-color=grey-3 color=blue-600")
            )
            dock_log = ui.log(max_lines=50).classes("terminal-window w-full h-32")
            with ui.row().classes("w-full justify-between pt-2"):

                async def stop_job():
                    global current_task_id
                    if current_task_id:
                        cancel_task(current_task_id)
                        dock_log.push("⚠️ JOB CANCELLED.")
                        dock_status_icon.classes(
                            remove="animate-spin", replace="text-red-500"
                        )
                        dock_status_icon.name = "cancel"
                        dock_title.text = "Cancelled"
                        current_task_id = None

                ui.button("STOP JOB", on_click=stop_job).props(
                    "flat dense color=red icon=stop"
                ).classes("w-1/2")
                dock_download = ui.button("Download", icon="download").classes(
                    "bg-green-600 text-white hidden w-1/2"
                )

    with ui.dialog() as save_dialog, ui.card().classes("w-96"):
        ui.label("Save Configuration").classes("text-lg font-bold mb-2")
        p_name = ui.input("Preset Name").classes("w-full")
        p_desc = ui.input("Description").classes("w-full")

        def commit_save():
            if not p_name.value:
                return ui.notify("Name required", type="warning")
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
            }
            PresetManager.save_preset(p_name.value, p_desc.value, config_dump, pid=None)
            ui.notify("✅ Preset Saved!", type="positive")
            preset_list.refresh()
            save_dialog.close()

        ui.button("Save", on_click=commit_save).classes(
            "w-full bg-blue-600 text-white mt-4"
        )

    with ui.dialog() as inspector_dialog, ui.card().classes(
        "w-full max-w-5xl h-3/4 p-0"
    ):
        with ui.row().classes("bg-slate-100 p-3 border-b"):
            ui.label("Inspector").classes("font-bold")
            ui.space()
            ui.button(icon="close", on_click=inspector_dialog.close).props(
                "flat round dense"
            )
        json_display = ui.code("Loading...", language="json").classes(
            "w-full h-full overflow-auto p-4 text-xs"
        )

    with ui.row().classes("w-full max-w-7xl mx-auto p-6 gap-6"):
        with ui.column().classes("w-3/5 gap-4"):
            with ui.card().classes("nice-card w-full p-0 overflow-hidden"):
                with ui.tabs().classes("w-full text-slate-600 bg-slate-50") as tabs:
                    tab_saved = ui.tab("Saved Profiles", icon="bookmarks")
                    tab_new = ui.tab("Connection Details", icon="settings_ethernet")

                with ui.tab_panels(tabs, value=tab_saved).classes("w-full p-6"):
                    with ui.tab_panel(tab_saved).classes("p-0 gap-4 flex flex-col"):

                        @ui.refreshable
                        def preset_list():
                            presets = PresetManager.load_presets()
                            if not presets:
                                ui.label("No saved profiles.").classes(
                                    "text-sm text-gray-400 italic"
                                )
                                return
                            for pid, p in presets.items():
                                with ui.row().classes(
                                    "w-full items-center border p-3 rounded hover:bg-slate-50"
                                ):
                                    with ui.column().classes("gap-0 flex-grow"):
                                        ui.label(p["name"]).classes(
                                            "font-bold text-slate-700"
                                        )
                                        ui.label(p["description"]).classes(
                                            "text-xs text-gray-400 truncate w-64"
                                        )

                                    async def load_this(pid=pid, p=p):
                                        global current_preset_id
                                        current_preset_id = pid
                                        ui.notify(
                                            f"Loading '{p['name']}'...",
                                            type="info",
                                            spinner=True,
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
                                                f"Connection Failed: {e}",
                                                type="negative",
                                            )
                                        if conf.get("use_join"):
                                            pt = conf.get("primary_table")
                                            st = conf.get("secondary_table")
                                            if pt in tables:
                                                p_table_sel.value = pt
                                                p_table_sel.update()
                                                p_col_sel.options = get_columns(eng, pt)
                                                p_col_sel.update()
                                                p_col_sel.value = conf.get(
                                                    "primary_col"
                                                )
                                                p_col_sel.update()
                                            if st in tables:
                                                s_table_sel.value = st
                                                s_table_sel.update()
                                                cols = get_columns(eng, st)
                                                for d in [
                                                    s_col_sel,
                                                    json_col_sel,
                                                    filter_col_sel,
                                                ]:
                                                    d.options = cols
                                                    d.update()
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
                                                for d in [
                                                    single_id_sel,
                                                    json_col_sel,
                                                    filter_col_sel,
                                                ]:
                                                    d.options = cols
                                                    d.update()
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
                                        ui.notify("✅ Loaded", type="positive")

                                    ui.button("Load", on_click=load_this).props(
                                        "flat dense color=blue"
                                    )
                                    ui.button(
                                        icon="delete",
                                        on_click=lambda pid=pid: [
                                            PresetManager.delete_preset(pid),
                                            preset_list.refresh(),
                                        ],
                                    ).props("flat dense color=red round")

                        preset_list()

                    with ui.tab_panel(tab_new).classes("p-0"):
                        with ui.column().classes("w-full gap-4"):
                            with ui.grid(columns=4).classes("w-full gap-2"):
                                host = ui.input("Host", value="localhost").props(
                                    "outlined dense"
                                )
                                user = ui.input("User", value="root").props(
                                    "outlined dense"
                                )
                                password = ui.input("Pass", password=True).props(
                                    "outlined dense"
                                )
                                dbname = ui.input("DB Name").props("outlined dense")

                            async def load_schema():
                                try:
                                    ui.notify(
                                        "Connecting...", type="info", spinner=True
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
                                    ui.notify(f"✅ Connected!", type="positive")
                                    step2.classes(
                                        remove="opacity-50 pointer-events-none grayscale"
                                    )
                                except Exception as e:
                                    ui.notify(f"Failed: {e}", type="negative")

                            ui.button(
                                "CONNECT / REFRESH SCHEMA", on_click=load_schema
                            ).classes("w-full bg-slate-800 text-white font-bold")

            with ui.card().classes(
                "nice-card w-full p-6 opacity-50 pointer-events-none grayscale transition-all"
            ) as step2:
                ui.label("CONFIGURATION").classes(
                    "text-xs font-bold text-slate-400 tracking-widest mb-4"
                )
                mode_group = ui.toggle(
                    {False: "Single", True: "Join"}, value=True
                ).props("no-caps push color=blue-9")

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

                with ui.grid(columns=2).bind_visibility_from(
                    mode_group, "value", value=True
                ).classes("w-full gap-2"):
                    p_table_sel = ui.select(
                        [], label="Primary Table", with_input=True
                    ).props("outlined dense")
                    p_col_sel = ui.select(
                        [], label="Primary Key", with_input=True
                    ).props("outlined dense")
                    p_table_sel.on_value_change(
                        lambda e: on_table_select(e, [p_col_sel])
                    )
                    s_table_sel = ui.select(
                        [], label="JSON Table", with_input=True
                    ).props("outlined dense")
                    s_col_sel = ui.select(
                        [], label="Foreign Key", with_input=True
                    ).props("outlined dense")
                    s_table_sel.on_value_change(
                        lambda e: on_table_select(
                            e, [s_col_sel, json_col_sel, filter_col_sel]
                        )
                    )

                with ui.grid(columns=2).bind_visibility_from(
                    mode_group, "value", value=False
                ).classes("w-full gap-2"):
                    single_table_sel = ui.select(
                        [], label="Target Table", with_input=True
                    ).props("outlined dense")
                    single_id_sel = ui.select(
                        [], label="ID Column", with_input=True
                    ).props("outlined dense")
                    single_table_sel.on_value_change(
                        lambda e: on_table_select(
                            e, [single_id_sel, json_col_sel, filter_col_sel]
                        )
                    )

                ui.separator().classes("my-4 opacity-50")
                with ui.grid(columns=3).classes("w-full gap-2"):
                    json_col_sel = ui.select(
                        [], label="JSON Column", with_input=True
                    ).props("outlined dense")
                    filter_col_sel = ui.select(
                        [], label="Filter Col", with_input=True
                    ).props("outlined dense")
                    filter_val = ui.input("Filter Value").props("outlined dense")

                with ui.expansion("Advanced (S3)", icon="cloud").classes("w-full mt-2"):
                    with ui.column().classes("p-2 w-full"):
                        is_s3 = ui.switch("Fetch from URL")
                        base_url = (
                            ui.input("Base URL")
                            .props("outlined dense")
                            .classes("w-full")
                            .bind_visibility_from(is_s3, "value")
                        )

            with ui.card().classes("nice-card w-full p-6 border-l-4 border-blue-500"):
                ui.label("FILTER").classes(
                    "text-xs font-bold text-blue-600 tracking-widest mb-2"
                )
                with ui.grid(columns=3).classes("w-full gap-2"):
                    date_col_sel = ui.select(
                        [], label="Date Column", with_input=True
                    ).props("outlined dense w-full")
                    start_date = ui.input("Start").props("type=date outlined dense")
                    end_date = ui.input("End").props("type=date outlined dense")

        with ui.column().classes("w-2/5 gap-4"):
            with ui.card().classes("nice-card w-full p-6 flex flex-col gap-4"):
                ui.label("OPERATIONS").classes(
                    "text-xs font-bold text-slate-400 tracking-widest"
                )

                async def run_test():
                    try:
                        ui.notify("Querying...", type="info", spinner=True)
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
                                        async with session.get(url, timeout=5) as resp:
                                            if resp.status == 200:
                                                raw = await resp.json()
                                            else:
                                                ui.notify(
                                                    "Fetch Failed", type="warning"
                                                )
                                                return
                                except:
                                    ui.notify("Fetch Error", type="negative")
                                    return
                            fmt = ParserEngine.identify_format(raw)
                            ui.notify(f"Format: {fmt}", type="positive")
                            content = str(raw)
                            if len(content) > 10000:
                                json_display.content = (
                                    content[:10000] + "\n...[TRUNCATED]..."
                                )
                            else:
                                try:
                                    json_display.content = json.dumps(raw, indent=2)
                                except:
                                    json_display.content = content
                            btn_run.enable()
                            btn_inspect.enable()
                            btn_save.enable()
                            if current_preset_id:
                                btn_update.enable()
                                btn_update.text = f"Update '{PresetManager.load_presets()[current_preset_id]['name']}'"
                        else:
                            ui.notify("0 Rows", type="warning")
                    except Exception as e:
                        ui.notify(str(e), type="negative")

                with ui.row().classes("w-full gap-2"):
                    ui.button("TEST", on_click=run_test).classes(
                        "flex-grow bg-slate-100 text-slate-700 font-bold"
                    )
                    btn_inspect = ui.button(
                        icon="visibility", on_click=inspector_dialog.open
                    ).props("flat dense color=blue")
                    btn_inspect.disable()

                async def start_job():
                    job_dock.classes(remove="hidden minimized")
                    dock_download.classes(add="hidden")
                    dock_status_icon.name = "sync"
                    dock_status_icon.classes(
                        add="animate-spin text-blue-500",
                        remove="text-green-500 text-red-500",
                    )
                    dock_title.text = "Initializing Job..."
                    dock_progress.set_value(0)
                    dock_log.clear()
                    dock_log.push("Starting Engine...")

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
                                    dock_title.text = st
                                dock_progress.set_value(
                                    res.info.get("progress", 0) / 100
                                )
                            elif res.state == "SUCCESS":
                                fetched = res.result.get("total_fetched", "?")
                                valid = res.result.get("total_rows", 0)
                                dock_log.push(
                                    f"✅ DONE. Fetched: {fetched} | Valid: {valid}"
                                )
                                dock_title.text = "Success"
                                dock_status_icon.name = "check_circle"
                                dock_status_icon.classes(
                                    remove="animate-spin text-blue-500",
                                    add="text-green-500",
                                )
                                dock_progress.set_value(1)
                                dock_download.props(
                                    f'href=/output/{os.path.basename(res.result["file_path"])}'
                                )
                                dock_download.classes(remove="hidden")
                                add_to_history(
                                    os.path.basename(res.result["file_path"]), valid
                                )
                                history_list.refresh()
                                current_task_id = None
                                break
                            elif res.state == "FAILURE" or res.state == "REVOKED":
                                dock_log.push(f"❌ STOPPED: {str(res.result)}")
                                dock_title.text = "Failed/Stopped"
                                dock_status_icon.name = "error"
                                dock_status_icon.classes(
                                    remove="animate-spin text-blue-500",
                                    add="text-red-500",
                                )
                                current_task_id = None
                                break
                    except RuntimeError:
                        return

                btn_run = ui.button("START", on_click=start_job).classes(
                    "w-full bg-blue-600 text-white font-bold shadow-lg h-12"
                )
                btn_run.disable()

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
                    }
                    old_data = PresetManager.load_presets()[current_preset_id]
                    PresetManager.save_preset(
                        old_data["name"],
                        old_data["description"],
                        config_dump,
                        pid=current_preset_id,
                    )
                    ui.notify(f"✅ Updated '{old_data['name']}'", type="positive")
                    preset_list.refresh()

                btn_update = (
                    ui.button("UPDATE PRESET", on_click=update_current_preset)
                    .props("flat dense text-color=green")
                    .classes("w-full border border-green-200")
                )
                btn_update.disable()
                btn_save = (
                    ui.button("SAVE AS NEW PRESET", on_click=save_dialog.open)
                    .props("flat dense text-color=purple")
                    .classes("w-full border border-purple-200")
                )
                btn_save.disable()

            with ui.card().classes("nice-card w-full p-6 flex-grow"):
                ui.label("HISTORY").classes(
                    "text-xs font-bold text-slate-400 tracking-widest"
                )

                @ui.refreshable
                def history_list():
                    for job in job_history:
                        with ui.row().classes(
                            "w-full justify-between items-center text-xs border-b py-2"
                        ):
                            with ui.column().classes("gap-0"):
                                ui.label(job["file"]).classes("font-bold truncate w-40")
                                ui.label(f"{job['rows']} rows • {job['time']}").classes(
                                    "text-gray-400"
                                )
                            ui.button(
                                icon="download",
                                on_click=lambda l=job["link"]: ui.download(l),
                            ).props("flat dense round color=green")

                history_list()


init_ui()
app.mount("/output", StaticFiles(directory="output"), name="output")
ui.run(title="Universal ETL v18.1", reload=False, port=8080)
