import json
import os
import uuid

PRESET_FILE = "presets.json"


class PresetManager:
    @staticmethod
    def load_presets():
        if not os.path.exists(PRESET_FILE):
            return {}
        try:
            with open(PRESET_FILE, "r") as f:
                return json.load(f)
        except:
            return {}

    @staticmethod
    def save_preset(name, description, config, pid=None):
        presets = PresetManager.load_presets()
        if not pid:
            pid = str(uuid.uuid4())[:8]

        presets[pid] = {
            "name": name,
            "description": description,
            "config": config,
            "updated_at": (
                str(os.path.getmtime(PRESET_FILE))
                if os.path.exists(PRESET_FILE)
                else ""
            ),
        }
        with open(PRESET_FILE, "w") as f:
            json.dump(presets, f, indent=4)
        return pid

    @staticmethod
    def delete_preset(pid):
        presets = PresetManager.load_presets()
        if pid in presets:
            del presets[pid]
            with open(PRESET_FILE, "w") as f:
                json.dump(presets, f, indent=4)
