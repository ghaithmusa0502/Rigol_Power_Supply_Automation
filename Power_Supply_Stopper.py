"""
Power Supply Stopper - A Tkinter application for controlling and logging data
from a programmable DC power supply, with automatic stop conditions.

This script provides a GUI to set voltage/current, monitor output, log data
to files, and automatically stop logging based on user-defined thresholds.
It supports both real instrument control via PyVISA and a simulation mode.
"""

import tkinter as tk
from tkinter import messagebox, filedialog, ttk, StringVar, BooleanVar
import os
import sys
import json
import csv
import queue
import threading
import time
import traceback
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, List, Any, Dict, Deque

# =============================================================================
# == Optional Third-Party Library Imports & Placeholders ==
# =============================================================================
# These are imported with error handling. We check for their existence
# before using features that depend on them.

try:
    import winsound
except ImportError:
    winsound = None
    print("Info: 'winsound' not found. Beep on stop disabled (Windows only).")

try:
    import pyvisa
except ImportError:
    pyvisa = None  # Flag that pyvisa is missing

try:
    import pandas as pd
except ImportError:
    pd = None  # Flag that pandas is missing

try:
    import openpyxl
except ImportError:
    openpyxl = None # Flag that openpyxl is missing

try:
    import numpy as np
except ImportError:
    # NumPy is considered critical for plotting, show error and exit.
    messagebox.showerror("Critical Dependency Missing", "NumPy not found. Please install (`pip install numpy`).")
    sys.exit(1)

try:
    import matplotlib
    matplotlib.use('TkAgg')
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    from matplotlib.figure import Figure
except ImportError:
    messagebox.showerror("Critical Dependency Missing", "Matplotlib not found. Please install (`pip install matplotlib`).")
    sys.exit(1)

try:
    from ttkthemes import ThemedTk
except ImportError:
    # Fallback to standard Tkinter if ttkthemes is missing
    print("Warning: 'ttkthemes' not found. Using default Tk styles.")
    ThemedTk = tk.Tk # Use standard Tk as ThemedTk

try:
    from PIL import Image, ImageTk
except ImportError:
    Image = None
    ImageTk = None
    print("Info: 'Pillow' not found. App icon features disabled.")


# =============================================================================
# == Constants ==
# =============================================================================
# --- File & Config Names ---
DEFAULT_CONFIG_FILENAME = "power_logger_config.json"
DEFAULT_PRESETS_FILENAME = "power_logger_presets.json"
APP_ICON_FILENAME = "app_icon.ico" # Optional icon file

# --- GUI Settings ---
DEFAULT_GUI_THEME = "clam"
DEFAULT_ENABLE_THEME_FADE = True
UI_PADDING_X = 10
UI_PADDING_Y = 10
FRAME_PADDING_X = 10
FRAME_PADDING_Y_TOP = 10
FRAME_PADDING_Y_BOTTOM = 5
WIDGET_PADDING_X = 5
WIDGET_PADDING_Y = 3
GRID_STICKY_W = tk.W
GRID_STICKY_EW = tk.EW
FRAME_STYLE = {'relief': 'sunken', 'borderwidth': 1}
STATUS_WRAPLENGTH = 800

# --- Plotting ---
DEFAULT_PLOT_STYLE = "default"
PLOT_FIGURE_SIZE = (8, 7)
PLOT_PAD_INCHES = 1.5
PLOT_RECT_MARGIN = [0, 0.03, 1, 0.97]
VOLTAGE_LINE_STYLE = 'b-'
CURRENT_LINE_STYLE = 'r-'
POWER_LINE_STYLE = 'g-'
RESISTANCE_LINE_STYLE = 'c-'
LINE_WIDTH = 1.5
LEGEND_LOCATION = 'upper right'
LEGEND_FONTSIZE = 'small'
MIN_PLOT_POINTS = 10

# --- Logging & Instrument ---
VISA_OPEN_TIMEOUT_MS = 5000
VISA_READ_WRITE_TIMEOUT_MS = 5000
MIN_UPDATE_INTERVAL_MS = 10
INITIAL_SETTLING_TIME_S = 1.0
ZERO_VI_STOP_DELAY_S = 1.0
ZERO_VI_THRESHOLD = 0.001
MODE_CONSTANT_VOLTAGE = "Constant Voltage"
MODE_CONSTANT_CURRENT = "Constant Current"

# --- Status Codes (Values are lowercase strings for tags/logs) ---
STATUS_INFO = "info"
STATUS_SUCCESS = "success"
STATUS_WARNING = "warning"
STATUS_ERROR = "error"
STATUS_DEBUG = "debug" # For detailed internal logging

# --- Export ---
DEFAULT_EXPORT_FORMAT = "csv"
VALID_EXPORT_FORMATS = ["csv", "xlsx", "json", "all"]

# --- Config Keys ---
CONFIG_RESOURCE_NAME = "resource_name"
CONFIG_VOLTAGE = "voltage"
CONFIG_CURRENT = "current"
CONFIG_THRESHOLD = "threshold"
CONFIG_STOP_CONDITION = "stop_condition"
CONFIG_UPDATE_INTERVAL = "update_interval_ms"
CONFIG_MAX_PLOT_POINTS = "max_plot_points"
CONFIG_SAVE_LOCATION = "save_location"
CONFIG_EXPORT_FORMAT = "export_format"
CONFIG_GUI_THEME = "gui_theme"
CONFIG_PLOT_STYLE = "plot_style"
CONFIG_SIMULATION_MODE = "simulation_mode"
CONFIG_NOTES = "notes"
CONFIG_ENABLE_THEME_FADE = "enable_theme_fade"
CONFIG_ANODE = "anode"
CONFIG_CATHODE = "cathode"
CONFIG_ELECTROLYTE = "electrolyte"
CONFIG_ELECTROLYTE_MOLARITY = "electrolyte_molarity"
CONFIG_OPERATION_MODE = "operation_mode"


# =============================================================================
# == Utility Classes & Functions ==
# =============================================================================
# (Could be moved to 'utils.py')

class Tooltip:
    """
    Enhanced Tooltip class for Tkinter widgets.
    Handles widget destruction and geometry errors more gracefully.
    """
    def __init__(self, widget, text=""):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.widget.bind("<Enter>", self.show_tooltip)
        self.widget.bind("<Leave>", self.hide_tooltip)
        self.widget.bind("<Destroy>", self.hide_tooltip) # Hide on destroy

    def set_text(self, text):
        """Updates the tooltip text."""
        self.text = text

    def show_tooltip(self, event=None):
        """Displays the tooltip window, handling potential errors."""
        if self.tooltip_window or not self.text:
            return

        try:
            # Check if widget still exists before proceeding
            if not self.widget.winfo_exists():
                return

            x, y, _, _ = self.widget.bbox("insert")
            x = x + self.widget.winfo_rootx() + 25
            y = y + self.widget.winfo_rooty() + 20

            self.tooltip_window = tk.Toplevel(self.widget)
            self.tooltip_window.wm_overrideredirect(True)
            self.tooltip_window.wm_geometry(f"+{x}+{y}")

            label = ttk.Label(self.tooltip_window, text=self.text,
                              background="#FFFFEA", relief=tk.SOLID,
                              borderwidth=1, wraplength=300)
            label.pack(ipadx=1)

        except tk.TclError:
            # Catch errors if widget disappears or bbox fails
            self.hide_tooltip()
        except Exception as e:
            print(f"Debug: Error showing tooltip: {e}")
            self.hide_tooltip()

    def hide_tooltip(self, event=None):
        """Hides the tooltip window if it exists."""
        if self.tooltip_window:
            try:
                if self.tooltip_window.winfo_exists():
                    self.tooltip_window.destroy()
            except tk.TclError:
                pass # Ignore if already destroyed
            finally:
                self.tooltip_window = None

def validate_float_input(value_str: str, name: str, allow_zero: bool = False) -> float:
    """Validates and converts a string to a non-negative float."""
    try:
        value = float(value_str)
        if value < 0:
            raise ValueError(f"{name} must be non-negative.")
        if not allow_zero and abs(value) < 1e-9:
            raise ValueError(f"{name} must be non-zero.")
        return value
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid {name}: '{value_str}'. Please enter a number. ({e})")

def validate_int_input(value_str: str, name: str, min_value: int) -> int:
    """Validates and converts a string to an integer above a minimum."""
    try:
        value = int(value_str)
        if value < min_value:
            raise ValueError(f"{name} must be at least {min_value}.")
        return value
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid {name}: '{value_str}'. Please enter an integer. ({e})")


# =============================================================================
# == Configuration Management ==
# =============================================================================
# (Could be moved to 'config.py')

class ConfigManager:
    """Handles loading, saving, and managing app configuration and presets."""

    def __init__(self, config_file: str = DEFAULT_CONFIG_FILENAME,
                 presets_file: str = DEFAULT_PRESETS_FILENAME):
        """Initializes ConfigManager, defines defaults, and loads files."""
        self.base_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        self.config_file = os.path.join(self.base_path, config_file)
        self.presets_file = os.path.join(self.base_path, presets_file)

        self.config: Dict[str, Any] = self._get_defaults()
        self.presets: Dict[str, Dict[str, Any]] = {}

        self.load_config()
        self.load_presets()

    def _get_defaults(self) -> Dict[str, Any]:
        """Returns the default configuration dictionary."""
        return {
            CONFIG_RESOURCE_NAME: "TCPIP0::192.168.1.100::INSTR", # Example TCP/IP
            CONFIG_VOLTAGE: 4.0,
            CONFIG_CURRENT: 0.5,
            CONFIG_THRESHOLD: 0.062,
            CONFIG_STOP_CONDITION: "below", # 'below' or 'above'
            CONFIG_UPDATE_INTERVAL: 200,
            CONFIG_MAX_PLOT_POINTS: 1000,
            CONFIG_SAVE_LOCATION: self.base_path,
            CONFIG_EXPORT_FORMAT: DEFAULT_EXPORT_FORMAT,
            CONFIG_GUI_THEME: DEFAULT_GUI_THEME,
            CONFIG_PLOT_STYLE: DEFAULT_PLOT_STYLE,
            CONFIG_SIMULATION_MODE: False,
            CONFIG_NOTES: "",
            CONFIG_ENABLE_THEME_FADE: DEFAULT_ENABLE_THEME_FADE,
            CONFIG_ANODE: "",
            CONFIG_CATHODE: "",
            CONFIG_ELECTROLYTE: "",
            CONFIG_ELECTROLYTE_MOLARITY: "",
            CONFIG_OPERATION_MODE: MODE_CONSTANT_VOLTAGE
        }

    def _load_json_file(self, filepath: str, description: str) -> Dict:
        """Helper to load a JSON file with error handling."""
        if not os.path.exists(filepath):
            print(f"Info: {description} file not found: '{filepath}'. Using defaults/empty.")
            return {}
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"Warning: Error decoding {description} file {filepath}: {e}. Using defaults/empty.")
            return {}
        except IOError as e:
            print(f"Warning: I/O error loading {description} file {filepath}: {e}. Using defaults/empty.")
            return {}
        except Exception as e:
            print(f"Warning: Unexpected error loading {description} file {filepath}: {e}. Using defaults/empty.")
            return {}

    def _save_json_file(self, filepath: str, data: Dict, description: str):
        """Helper to save a JSON file with error handling."""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except IOError as e:
            print(f"Error: Could not save {description} to {filepath}: {e}")
            messagebox.showerror("Save Error", f"Could not save {description} to file:\n{filepath}\n\n{e}")
        except Exception as e:
            print(f"Error: Unexpected error saving {description}: {e}")
            messagebox.showerror("Save Error", f"Unexpected error saving {description}:\n{e}")


    def load_config(self) -> None:
        """Loads config, merging with defaults to handle new keys."""
        loaded_config = self._load_json_file(self.config_file, "Configuration")
        defaults = self._get_defaults()
        defaults.update(loaded_config) # Loaded values override defaults
        self.config = defaults

    def save_config(self) -> None:
        """Saves the current configuration."""
        self._save_json_file(self.config_file, self.config, "Configuration")

    def load_presets(self) -> None:
        """Loads presets."""
        self.presets = self._load_json_file(self.presets_file, "Presets")

    def save_presets(self) -> None:
        """Saves the current presets."""
        self._save_json_file(self.presets_file, self.presets, "Presets")

    def add_preset(self, name: str, preset_data: Dict[str, Any]) -> bool:
        """Adds or updates a preset."""
        if not name:
            messagebox.showwarning("Preset Error", "Preset name cannot be empty.")
            return False
        # Only save relevant keys
        keys_to_save = [
            CONFIG_RESOURCE_NAME, CONFIG_VOLTAGE, CONFIG_CURRENT,
            CONFIG_THRESHOLD, CONFIG_STOP_CONDITION, CONFIG_EXPORT_FORMAT,
            CONFIG_ANODE, CONFIG_CATHODE, CONFIG_ELECTROLYTE,
            CONFIG_ELECTROLYTE_MOLARITY, CONFIG_OPERATION_MODE
        ]
        self.presets[name] = {k: preset_data.get(k) for k in keys_to_save}
        self.save_presets()
        return True

    def get_preset(self, name: str) -> Optional[Dict[str, Any]]:
        """Retrieves a preset by name."""
        return self.presets.get(name)

    def delete_preset(self, name: str) -> bool:
        """Deletes a preset."""
        if name in self.presets:
            del self.presets[name]
            self.save_presets()
            return True
        return False

    def get_preset_names(self) -> List[str]:
        """Returns a sorted list of preset names."""
        return sorted(self.presets.keys())

    def get_notes(self) -> str:
        """Gets notes from config."""
        return self.config.get(CONFIG_NOTES, "")

    def save_notes(self, notes_content: str):
        """Saves notes to config (does not save file immediately)."""
        self.config[CONFIG_NOTES] = notes_content


# =============================================================================
# == Data Management & Export ==
# =============================================================================
# (Could be moved to 'data_handler.py')

class DataManager:
    """Manages data queues and deques for logging and plotting."""
    def __init__(self, max_plot_points: int = 1000):
        self.max_plot_points = max(max_plot_points, MIN_PLOT_POINTS)
        self.data_queue: queue.Queue[List[float]] = queue.Queue()
        self.time_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.voltage_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.current_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.power_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.resistance_data: Deque[float] = deque(maxlen=self.max_plot_points)

    def put(self, data_point: List[float]): self.data_queue.put(data_point)
    def get_nowait(self) -> List[float]: return self.data_queue.get_nowait()
    def empty(self) -> bool: return self.data_queue.empty()

    def append_for_plotting(self, t: float, v: float, i: float, p: float, r: float):
        """Appends data to deques for the plot's sliding window."""
        self.time_data.append(t); self.voltage_data.append(v)
        self.current_data.append(i); self.power_data.append(p)
        self.resistance_data.append(r)

    def clear_plot_data(self):
        """Clears only the plotting deques."""
        self.time_data.clear(); self.voltage_data.clear()
        self.current_data.clear(); self.power_data.clear()
        self.resistance_data.clear()

class DataExporter:
    """Handles exporting logged data to various file formats."""

    HEADERS = ["Time (s)", "Voltage (V)", "Current (A)", "Power (W)", "Resistance (Ω)"]

    @staticmethod
    def _add_metadata_to_csv(writer: csv.writer, config: Dict, notes: str):
        """Writes configuration and notes as comments to a CSV file."""
        writer.writerow(['# --- Configuration ---'])
        cfg_keys = [CONFIG_RESOURCE_NAME, CONFIG_OPERATION_MODE, CONFIG_VOLTAGE,
                    CONFIG_CURRENT, CONFIG_THRESHOLD, CONFIG_STOP_CONDITION,
                    CONFIG_ANODE, CONFIG_CATHODE, CONFIG_ELECTROLYTE,
                    CONFIG_ELECTROLYTE_MOLARITY]
        for k in cfg_keys:
            writer.writerow([f'# {k.upper()}: {config.get(k, "N/A")}'])
        writer.writerow(['# --- Notes ---'])
        for line in notes.splitlines():
            writer.writerow([f'# {line}'])
        writer.writerow(['# --- Data ---'])

    @staticmethod
    def save_to_csv(filepath: str, data: List, config: Dict, notes: str) -> Tuple[bool, str]:
        """Saves data to a CSV file."""
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                DataExporter._add_metadata_to_csv(writer, config, notes)
                writer.writerow(DataExporter.HEADERS)
                writer.writerows(data)
            return True, f"CSV saved: {os.path.basename(filepath)}"
        except IOError as e:
            return False, f"CSV I/O Error: {e}"
        except Exception as e:
            return False, f"CSV Save Error: {e}"

    @staticmethod
    def save_to_excel(filepath: str, data: List, config: Dict, notes: str) -> Tuple[bool, str]:
        """Saves data and metadata to an Excel file."""
        if not pd or not openpyxl:
            return False, "Excel export requires 'pandas' and 'openpyxl'."
        try:
            df_data = pd.DataFrame(data, columns=DataExporter.HEADERS)
            cfg_data = {k: config.get(k, "N/A") for k in [
                CONFIG_VOLTAGE, CONFIG_CURRENT, CONFIG_OPERATION_MODE,
                CONFIG_THRESHOLD, CONFIG_STOP_CONDITION, CONFIG_ANODE,
                CONFIG_CATHODE, CONFIG_ELECTROLYTE, CONFIG_ELECTROLYTE_MOLARITY]}
            config_df = pd.DataFrame.from_dict(cfg_data, orient='index', columns=['Value'])
            notes_df = pd.DataFrame(notes.splitlines(), columns=["Notes"])

            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df_data.to_excel(writer, sheet_name='Data', index=False)
                config_df.to_excel(writer, sheet_name='Settings', index=True)
                notes_df.to_excel(writer, sheet_name='Notes', index=False)
            return True, f"Excel saved: {os.path.basename(filepath)}"
        except (ImportError, NameError):
             return False, "Excel export requires 'pandas' and 'openpyxl'."
        except Exception as e:
            return False, f"Excel Save Error: {e}"

    @staticmethod
    def save_to_json(filepath: str, data: List, config: Dict, notes: str) -> Tuple[bool, str]:
        """Saves data and metadata to a JSON file."""
        if not pd:
             return False, "JSON export requires 'pandas'."
        try:
            df_data = pd.DataFrame(data, columns=DataExporter.HEADERS)
            cfg_data = {k: config.get(k, "N/A") for k in [
                CONFIG_VOLTAGE, CONFIG_CURRENT, CONFIG_OPERATION_MODE,
                CONFIG_THRESHOLD, CONFIG_STOP_CONDITION, CONFIG_ANODE,
                CONFIG_CATHODE, CONFIG_ELECTROLYTE, CONFIG_ELECTROLYTE_MOLARITY]}
            json_output = {
                "settings": cfg_data,
                "notes": notes,
                "data": df_data.to_dict(orient="records")
            }
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_output, f, indent=4)
            return True, f"JSON saved: {os.path.basename(filepath)}"
        except (ImportError, NameError):
             return False, "JSON export requires 'pandas'."
        except Exception as e:
            return False, f"JSON Save Error: {e}"

    @staticmethod
    def export_data(base_filename: str, data: List, config: Dict, notes: str,
                      status_queue: queue.Queue):
        """Exports data to formats specified in config."""
        if not data:
            status_queue.put((STATUS_WARNING, "No data logged to export."))
            return

        export_format = config.get(CONFIG_EXPORT_FORMAT, DEFAULT_EXPORT_FORMAT)
        save_map = {
            "csv": DataExporter.save_to_csv,
            "xlsx": DataExporter.save_to_excel,
            "json": DataExporter.save_to_json
        }

        formats_to_save = save_map.keys() if export_format == "all" else [export_format]

        for fmt in formats_to_save:
            if fmt in save_map:
                filepath = f"{base_filename}.{fmt}"
                status_queue.put((STATUS_INFO, f"Saving to {fmt.upper()}..."))
                success, message = save_map[fmt](filepath, data, config, notes)
                status_queue.put((STATUS_SUCCESS if success else STATUS_ERROR, message))


# =============================================================================
# == Data Logger ==
# =============================================================================
# (Could be moved to 'logger.py')

class DataLogger:
    """Handles instrument communication, simulation, and data logging."""

    def __init__(self, config: Dict, data_manager: DataManager,
                 status_queue: queue.Queue):
        """Initializes the logger."""
        self.config = config
        self.data_manager = data_manager
        self.status_queue = status_queue
        self.dp: Optional[pyvisa.Resource] = None
        self.rm: Optional[pyvisa.ResourceManager] = None
        self.stop_event = threading.Event()
        self.logging_thread: Optional[threading.Thread] = None
        self.is_simulating = self.config.get(CONFIG_SIMULATION_MODE, False)
        self._logged_data: List[List[float]] = []
        self._sim_start_time: Optional[float] = None

    def _send_status(self, level: str, message: str):
        """Helper to send status messages."""
        self.status_queue.put((level, message))

    def connect(self) -> bool:
        """Connects to the VISA instrument or sets up simulation."""
        if self.is_simulating:
            self._send_status(STATUS_INFO, "Simulation mode active.")
            return True

        if not pyvisa:
            self._send_status(STATUS_ERROR, "PyVISA library not found. Cannot connect.")
            return False

        resource_name = self.config.get(CONFIG_RESOURCE_NAME)
        if not resource_name:
            self._send_status(STATUS_ERROR, "No VISA resource selected.")
            return False

        try:
            self._send_status(STATUS_INFO, f"Connecting to: {resource_name}")
            self.rm = pyvisa.ResourceManager()
            self.dp = self.rm.open_resource(resource_name, open_timeout=VISA_OPEN_TIMEOUT_MS)
            self.dp.timeout = VISA_READ_WRITE_TIMEOUT_MS
            self.dp.read_termination = '\n'
            self.dp.write_termination = '\n'
            idn = self.dp.query('*IDN?').strip()
            self._send_status(STATUS_SUCCESS, f"Connected to: {idn}")
            return True
        except pyvisa.errors.VisaIOError as e:
            self._send_status(STATUS_ERROR, f"VISA I/O Error: {e}")
            self._close_connection()
            return False
        except Exception as e:
            self._send_status(STATUS_ERROR, f"Connection error: {e}")
            print(traceback.format_exc())
            self._close_connection()
            return False

    def _close_connection(self):
        """Safely closes the instrument connection."""
        if self.dp:
            try:
                # Try turning off output, but don't fail if already closed
                try: self.dp.write(":OUTP OFF")
                except Exception: pass
                self.dp.close()
                self._send_status(STATUS_INFO, "Instrument connection closed.")
            except Exception as e:
                self._send_status(STATUS_ERROR, f"Error closing instrument: {e}")
            finally: self.dp = None
        if self.rm:
            try: self.rm.close()
            except Exception: pass
            finally: self.rm = None

    def _setup_instrument(self) -> bool:
        """Sets up the instrument based on current config."""
        v = self.config.get(CONFIG_VOLTAGE, 0.0)
        c = self.config.get(CONFIG_CURRENT, 0.0)
        mode = self.config.get(CONFIG_OPERATION_MODE, MODE_CONSTANT_VOLTAGE)
        t = self.config.get(CONFIG_THRESHOLD, 0.0)

        try:
            if mode == MODE_CONSTANT_CURRENT:
                self.dp.write(f":VOLT:PROT {t}")
                self.dp.write(":OUTP:OVP ON")
                self._send_status(STATUS_INFO, f"OVP ON. V Limit: {t}V.")
            else: # CV mode
                self.dp.write(":OUTP:OVP OFF")
                self._send_status(STATUS_INFO, "OVP OFF.")

            self.dp.write(f":APPL CH1,{v},{c}")
            self.dp.write(":OUTP CH1,ON")
            self._send_status(STATUS_INFO, f"Set: {v}V, {c}A, ON. Mode: {mode}")
            return True
        except pyvisa.errors.VisaIOError as e:
            self._send_status(STATUS_ERROR, f"VISA Error during setup: {e}")
            return False
        except Exception as e:
            self._send_status(STATUS_ERROR, f"Setup Error: {e}")
            return False


    def start(self) -> bool:
        """Starts the logging thread."""
        self.stop_event.clear()
        if self.logging_thread and self.logging_thread.is_alive():
            self._send_status(STATUS_WARNING, "Logging already running.")
            return False

        if not self.is_simulating:
            if not self.dp:
                self._send_status(STATUS_ERROR, "Not connected.")
                return False
            if not self._setup_instrument():
                self.stop() # Ensure cleanup if setup fails
                return False
        else:
            self._sim_start_time = time.time()
            self._send_status(STATUS_INFO, "Simulation started.")

        self._logged_data = [] # Clear previous data
        self.logging_thread = threading.Thread(target=self._log_data_loop, daemon=True)
        self.logging_thread.start()
        return True

    def stop(self, notes_content: str = ""):
        """Stops logging, exports data, and closes connections."""
        if not self.stop_event.is_set():
            self._send_status(STATUS_INFO, "Stop requested...")
            self.stop_event.set()

        if self.logging_thread and self.logging_thread.is_alive():
            self.logging_thread.join(timeout=5.0) # Wait a bit longer

        base_filename = self._generate_base_filename()
        DataExporter.export_data(base_filename, self._logged_data,
                                 self.config, notes_content, self.status_queue)

        if not self.is_simulating:
            self._close_connection()
        else:
            self._send_status(STATUS_INFO, "Simulation stopped.")

        self.logging_thread = None
        self._send_status(STATUS_SUCCESS, f"Logger stopped. {len(self._logged_data)} points recorded.")

    def _generate_base_filename(self) -> str:
        """Generates a base filename for exports."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        v = str(self.config.get(CONFIG_VOLTAGE, 0.0)).replace('.', '_')
        c = str(self.config.get(CONFIG_CURRENT, 0.0)).replace('.', '_')
        loc = self.config.get(CONFIG_SAVE_LOCATION, "").strip() # Get and strip whitespace

        # --- START ADDED VALIDATION ---
        if not loc: # If location is empty after stripping
            # Default to the directory of the script or the current working directory
            # Using self.config_manager.base_path if DataLogger has access or a known default
            # For simplicity, let's assume we can use a 'logs' subdirectory in the app's base path
            # This requires DataLogger to know the base_path.
            # A robust way would be for PowerLoggerApp to pass its config_manager.base_path
            # or to set a sensible default during ConfigManager initialization.
            # For an immediate fix within DataLogger if it doesn't have base_path:
            try:
                # Attempt to use the script's directory
                script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
                loc = os.path.join(script_dir, "logs") # Default to a 'logs' subdirectory
                self._send_status(STATUS_WARNING, f"Save location was empty. Defaulting to: {loc}")
            except Exception:
                # Fallback to current working directory if sys.argv[0] is problematic (e.g. in frozen app)
                loc = os.path.join(os.getcwd(), "logs")
                self._send_status(STATUS_WARNING, f"Save location was empty. Defaulting to: {loc}")
        os.makedirs(loc, exist_ok=True)
        return os.path.join(loc, f"Rigol Power Supply V{v}_A{c}_{ts}")

    def _read_instrument(self) -> Tuple[float, float]:
        """Reads voltage and current from the instrument."""
        v = float(self.dp.query(":MEAS:VOLT? CH1"))
        c = float(self.dp.query(":MEAS:CURR? CH1"))
        return v, c

    def _read_simulation(self, elapsed_time: float) -> Tuple[float, float]:
        """Generates simulated voltage and current."""
        v = self.config.get(CONFIG_VOLTAGE, 4.0)
        c_set = self.config.get(CONFIG_CURRENT, 0.5)
        # Simulate current decay (or change based on mode later if needed)
        c = max(0, c_set - 0.05 * elapsed_time + np.random.randn() * 0.005)
        return v, c

    def _check_stop_condition(self, v: float, i: float) -> bool:
        """Checks if the auto-stop conditions are met."""
        mode = self.config.get(CONFIG_OPERATION_MODE)
        threshold = self.config.get(CONFIG_THRESHOLD)
        condition = self.config.get(CONFIG_STOP_CONDITION) # 'below' or 'above'

        value_to_check = abs(i) if mode == MODE_CONSTANT_VOLTAGE else v
        limit_type = "Current" if mode == MODE_CONSTANT_VOLTAGE else "Voltage"
        unit = "A" if mode == MODE_CONSTANT_VOLTAGE else "V"

        stop = False
        if condition.lower() == "below" and value_to_check < threshold:
            self._send_status(STATUS_WARNING, f"Stop: {limit_type} {value_to_check:.4f} < {threshold:.4f} {unit}.")
            stop = True
        elif condition.lower() == "above" and value_to_check > threshold:
            self._send_status(STATUS_WARNING, f"Stop: {limit_type} {value_to_check:.4f} > {threshold:.4f} {unit}.")
            stop = True

        return stop

    def _log_data_loop(self):
        """The main data logging loop running in a thread."""
        try:
            interval_ms = self.config.get(CONFIG_UPDATE_INTERVAL, 200)
            delay_s = max(interval_ms, MIN_UPDATE_INTERVAL_MS) / 1000.0
            start_time = time.time()
            points = 0
            self._send_status(STATUS_INFO, f"Logging started (Interval: {delay_s*1000:.0f}ms).")

            while not self.stop_event.is_set():
                loop_start = time.perf_counter()
                now = time.time(); et = now - start_time
                v_m, c_m = 0.0, 0.0

                try:
                    v_m, c_m = self._read_simulation(et) if self.is_simulating else self._read_instrument()
                except pyvisa.errors.VisaIOError as e:
                    self._send_status(STATUS_ERROR, f"VISA Read Error: {e}. Stopping.")
                    self.stop_event.set(); break
                except Exception as e:
                    self._send_status(STATUS_ERROR, f"Read Error: {e}. Stopping.")
                    print(traceback.format_exc())
                    self.stop_event.set(); break

                p = v_m * c_m
                r = v_m / c_m if abs(c_m) > 1e-9 else float('inf')
                row = [et, v_m, c_m, p, r]
                self._logged_data.append(row)
                self.data_manager.put(row)
                points += 1

                # --- Stop Condition Checks ---
                stop_now = False
                # 1. Zero V/I Check (after delay)
                if et > ZERO_VI_STOP_DELAY_S and abs(v_m) < ZERO_VI_THRESHOLD and abs(c_m) < ZERO_VI_THRESHOLD:
                    self._send_status(STATUS_WARNING, "Zero V & I detected. Stopping.")
                    stop_now = True
                # 2. Threshold Check (after settling time)
                elif et > INITIAL_SETTLING_TIME_S:
                    stop_now = self._check_stop_condition(v_m, c_m)

                if stop_now:
                    if not self.is_simulating and self.dp:
                        try: self.dp.write(":OUTP CH1,OFF")
                        except Exception: pass # Ignore error if already off/bad
                    if winsound: 
                        try: winsound.Beep(1000, 500)
                        except Exception: pass
                    self.stop_event.set()
                    self._send_status("STOP_SIGNAL", "Auto-Stop Triggered") # Special signal
                    break # Exit loop immediately

                # --- Sleep ---
                sleep_dur = delay_s - (time.perf_counter() - loop_start)
                if sleep_dur > 0: time.sleep(sleep_dur)

        except Exception as e:
            self._send_status(STATUS_ERROR, f"Critical Log Loop Error: {e}")
            print(traceback.format_exc())
        finally:
            self._send_status("LOGGER_FINISHED", "Logger thread has ended.")


# =============================================================================
# == GUI Application ==
# =============================================================================
# (Could be moved to 'gui.py', with `PowerLoggerApp` becoming the main controller)

class PowerLoggerApp:
    """The main Tkinter application class."""

    def __init__(self, root: ThemedTk):
        """Initializes the GUI, config, logger, and other components."""
        self.root = root
        self.config_manager = ConfigManager()
        self.data_manager = DataManager(self.config_manager.config.get(CONFIG_MAX_PLOT_POINTS))
        self.status_queue = queue.Queue()
        self.logger: Optional[DataLogger] = None
        self.is_logging = False
        self.executor = ThreadPoolExecutor(max_workers=3) # Allow threads for scan, log, etc.
        self._after_check_queues_id: Optional[str] = None
        self.ani: Optional[animation.FuncAnimation] = None

        # --- Check Critical Dependencies ---
        if not pyvisa:
            messagebox.showwarning("Missing Dependency",
                                 "PyVISA not found. Instrument control is disabled. "
                                 "Please install (`pip install pyvisa`) and restart.")
            self.config_manager.config[CONFIG_SIMULATION_MODE] = True # Force sim

        # --- Tkinter Variables ---
        self._setup_variables()

        # --- Build UI ---
        try:
            self._setup_variables()
            self.root.title("Power Supply Stopper")
            self.root.geometry("950x750"); self.root.minsize(800, 600)
            self._set_icon()
            self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
            self._setup_ui()
            self._apply_theme(self.config_manager.config.get(CONFIG_GUI_THEME))
            self._load_initial_values()
            self._create_plot_figure()
            self._create_plot_canvas()
            self._create_plot_lines()
            self._create_plot_toolbar()
            self.check_queues() # Start queue polling
            self.scan_visa_resources(auto_scan=True) # Auto-scan on launch
            self.update_ui_state(self.is_logging)
        except Exception as e:
            self._handle_initialization_error(e)

    def _setup_variables(self):
        """Initializes all Tkinter StringVars, BooleanVars, etc."""
        self.resource_var = StringVar()
        self.voltage_var = StringVar()
        self.current_var = StringVar()
        self.threshold_var = StringVar()
        self.stop_condition_var = StringVar()
        self.save_location_var = StringVar()
        self.export_format_var = StringVar()
        self.status_var = StringVar(value="Ready")
        self.update_interval_var = StringVar()
        self.max_plot_points_var = StringVar()
        self.gui_theme_var = StringVar()
        self.plot_style_var = StringVar()
        self.simulation_mode_var = BooleanVar()
        self.enable_theme_fade_var = BooleanVar()
        self.anode_var = StringVar()
        self.cathode_var = StringVar()
        self.electrolyte_var = StringVar()
        self.electrolyte_molarity_var = StringVar()
        self.preset_name_var = StringVar()
        self.operation_mode_var = StringVar()

    def _load_initial_values(self):
        """Loads configuration values into Tkinter variables."""
        cfg = self.config_manager.config
        self.resource_var.set(cfg.get(CONFIG_RESOURCE_NAME, ""))
        self.voltage_var.set(str(cfg.get(CONFIG_VOLTAGE)))
        self.current_var.set(str(cfg.get(CONFIG_CURRENT)))
        self.threshold_var.set(str(cfg.get(CONFIG_THRESHOLD)))
        self.stop_condition_var.set(cfg.get(CONFIG_STOP_CONDITION))
        self.save_location_var.set(cfg.get(CONFIG_SAVE_LOCATION))
        self.export_format_var.set(cfg.get(CONFIG_EXPORT_FORMAT))
        self.update_interval_var.set(str(cfg.get(CONFIG_UPDATE_INTERVAL)))
        self.max_plot_points_var.set(str(cfg.get(CONFIG_MAX_PLOT_POINTS)))
        self.gui_theme_var.set(cfg.get(CONFIG_GUI_THEME))
        self.plot_style_var.set(cfg.get(CONFIG_PLOT_STYLE))
        self.simulation_mode_var.set(cfg.get(CONFIG_SIMULATION_MODE))
        self.enable_theme_fade_var.set(cfg.get(CONFIG_ENABLE_THEME_FADE))
        self.anode_var.set(cfg.get(CONFIG_ANODE, ""))
        self.cathode_var.set(cfg.get(CONFIG_CATHODE, ""))
        self.electrolyte_var.set(cfg.get(CONFIG_ELECTROLYTE, ""))
        self.electrolyte_molarity_var.set(cfg.get(CONFIG_ELECTROLYTE_MOLARITY, ""))
        self.operation_mode_var.set(cfg.get(CONFIG_OPERATION_MODE))
        self.load_notes()

    def _handle_initialization_error(self, error):
        """Displays a critical error during startup and exits."""
        print("CRITICAL ERROR DURING INITIALIZATION:")
        traceback.print_exc()
        messagebox.showerror("Initialization Error", f"Failed to start:\n\n{error}")
        if self.root and self.root.winfo_exists():
            self.root.destroy()
        sys.exit(1)

    def _set_icon(self):
        """Sets the window icon if Pillow is available and file exists."""
        if not (Image and ImageTk): return
        try:
            icon_path = os.path.join(self.config_manager.base_path, APP_ICON_FILENAME)
            if os.path.exists(icon_path):
                pil_image = Image.open(icon_path)
                tk_image = ImageTk.PhotoImage(pil_image)
                self.root.iconphoto(True, tk_image)
            else:
                print(f"Info: Icon file '{APP_ICON_FILENAME}' not found.")
        except Exception as e:
            print(f"Warning: Could not set window icon: {e}.")

    def _setup_ui(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=UI_PADDING_X, pady=UI_PADDING_Y)
        tabs = {
            "Control": ttk.Frame(self.notebook),
            "Plots": ttk.Frame(self.notebook),
            "Settings": ttk.Frame(self.notebook),
            "Log": ttk.Frame(self.notebook),
            "Notes": ttk.Frame(self.notebook),
        }
        for name, tab in tabs.items():
            self.notebook.add(tab, text=f" {name} ")

        self.main_tab, self.plot_tab, self.settings_tab, self.log_tab, self.notes_tab = tabs.values()

        # 🛠 Set up all tabs before load_presets_listbox() or update_ui_state()
        self._setup_control_tab()
        self._setup_plot_tab()
        self._setup_notes_tab()    # MOVE THIS BEFORE settings_tab
        self._setup_log_tab()
        self._setup_settings_tab()  # This will call load_presets_listbox()


    def _create_label_entry(self, parent, label_text, row, var, tooltip_text="", style=FRAME_STYLE, col=1, **kwargs):
        """Helper to create a Label and Entry pair."""
        ttk.Label(parent, text=label_text).grid(row=row, column=0, sticky=GRID_STICKY_W, padx=WIDGET_PADDING_X, pady=WIDGET_PADDING_Y)
        f = ttk.Frame(parent, **style); f.grid(row=row, column=col, sticky=GRID_STICKY_EW, padx=WIDGET_PADDING_X, pady=WIDGET_PADDING_Y, **kwargs)
        entry = ttk.Entry(f, textvariable=var); entry.pack(fill=tk.X, expand=True)
        Tooltip(entry, tooltip_text)
        return entry

    def _setup_control_tab(self):
        """Builds the Control Tab UI."""
        cf = ttk.LabelFrame(self.main_tab, text="Power Supply Control")
        cf.pack(fill=tk.X, padx=UI_PADDING_X, pady=(FRAME_PADDING_Y_TOP, FRAME_PADDING_Y_BOTTOM))
        cif = ttk.LabelFrame(self.main_tab, text="Cell Information")
        cif.pack(fill=tk.X, padx=UI_PADDING_X, pady=FRAME_PADDING_Y_BOTTOM)
        sf = ttk.LabelFrame(self.main_tab, text="Status")
        sf.pack(fill=tk.X, padx=UI_PADDING_X, pady=FRAME_PADDING_Y_BOTTOM)
        bf = ttk.Frame(self.main_tab)
        bf.pack(fill=tk.X, padx=UI_PADDING_X, pady=(FRAME_PADDING_Y_BOTTOM, UI_PADDING_Y))

        # --- Control Frame ---
        r = 0
        ttk.Label(cf, text="VISA Resource:").grid(row=r, column=0, sticky=GRID_STICKY_W, padx=5, pady=3)
        f = ttk.Frame(cf, **FRAME_STYLE); f.grid(row=r, column=1, columnspan=2, sticky=GRID_STICKY_EW, padx=5, pady=3)
        self.resource_combobox = ttk.Combobox(f, textvariable=self.resource_var, state="readonly")
        self.resource_combobox.pack(fill=tk.X, expand=True)
        self.scan_button = ttk.Button(cf, text="Scan", command=self.scan_visa_resources, width=6)
        self.scan_button.grid(row=r, column=3, padx=5, pady=3); r += 1

        ttk.Label(cf, text="Operation Mode:").grid(row=r, column=0, sticky=GRID_STICKY_W, padx=5, pady=3);
        f = ttk.Frame(cf); f.grid(row=r, column=1, columnspan=3, sticky=GRID_STICKY_W, padx=5, pady=3)
        self.operation_mode_radios = [ttk.Radiobutton(f, text=m, variable=self.operation_mode_var, value=m, command=self._update_threshold_label) for m in [MODE_CONSTANT_VOLTAGE, MODE_CONSTANT_CURRENT]]
        [rd.pack(side=tk.LEFT, padx=(0, 10)) for rd in self.operation_mode_radios]; Tooltip(f, "Select CV or CC mode."); r += 1

        self.voltage_entry = self._create_label_entry(cf, "Voltage (V):", r, self.voltage_var)
        self.voltage_tooltip = Tooltip(self.voltage_entry); r += 1
        self.current_entry = self._create_label_entry(cf, "Current (A):", r, self.current_var)
        self.current_tooltip = Tooltip(self.current_entry); r += 1
        self.threshold_label_widget = ttk.Label(cf, text="Threshold:") # Text set by _update_threshold_label
        self.threshold_label_widget.grid(row=r, column=0, sticky=GRID_STICKY_W, padx=5, pady=3)
        f = ttk.Frame(cf, **FRAME_STYLE); f.grid(row=r, column=1, sticky=GRID_STICKY_EW, padx=5, pady=3)
        self.threshold_entry = ttk.Entry(f, textvariable=self.threshold_var); self.threshold_entry.pack(fill=tk.X, expand=True)
        self.threshold_tooltip = Tooltip(self.threshold_entry); r += 1

        self.save_location_entry = self._create_label_entry(cf, "Save Location:", r, self.save_location_var, "Directory for log files", columnspan=2)
        self.browse_button = ttk.Button(cf, text="Browse...", command=self.browse_save_location, width=10)
        self.browse_button.grid(row=r, column=3, padx=5, pady=3); r += 1

        ttk.Label(cf, text="Export Format:").grid(row=r, column=0, sticky=GRID_STICKY_W, padx=5, pady=3)
        f = ttk.Frame(cf); f.grid(row=r, column=1, columnspan=3, sticky=GRID_STICKY_W, padx=5, pady=3)
        self.export_format_radios = [ttk.Radiobutton(f, text=fmt.upper(), variable=self.export_format_var, value=fmt) for fmt in VALID_EXPORT_FORMATS]
        [rd.pack(side=tk.LEFT, padx=(0, 10)) for rd in self.export_format_radios]; r += 1
        cf.columnconfigure(1, weight=1)

        # --- Cell Info Frame ---
        cr = 0
        self.anode_entry = self._create_label_entry(cif, "Anode:", cr, self.anode_var, "Anode Material"); cr += 1
        self.cathode_entry = self._create_label_entry(cif, "Cathode:", cr, self.cathode_var, "Cathode Material"); cr += 1
        self.electrolyte_entry = self._create_label_entry(cif, "Electrolyte:", cr, self.electrolyte_var, "Electrolyte Composition"); cr += 1
        self.electrolyte_molarity_entry = self._create_label_entry(cif, "Molarity (M):", cr, self.electrolyte_molarity_var, "Electrolyte Molarity"); cr += 1
        cif.columnconfigure(1, weight=1)

        # --- Status & Buttons ---
        self.status_indicator = ttk.Label(sf, textvariable=self.status_var, anchor=tk.W, wraplength=STATUS_WRAPLENGTH)
        self.status_indicator.pack(fill=tk.X, padx=5, pady=3)
        self.start_button = ttk.Button(bf, text="Start Logging", command=self.start_logging)
        self.start_button.pack(side=tk.LEFT, padx=5)
        self.stop_button = ttk.Button(bf, text="Stop Logging", command=self.stop_logging, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)
        self._update_threshold_label() # Set initial tooltips

    def _setup_settings_tab(self):
        """Builds the Settings Tab UI."""
        p = {'padx': WIDGET_PADDING_X, 'pady': WIDGET_PADDING_Y}; g = {**p, 'sticky': GRID_STICKY_W}; ge = {**p, 'sticky': GRID_STICKY_EW}
        gsf = ttk.LabelFrame(self.settings_tab, text="General Settings"); gsf.pack(fill=tk.X, padx=FRAME_PADDING_X, pady=(FRAME_PADDING_Y_TOP, FRAME_PADDING_Y_BOTTOM))
        r = 0
        self.update_interval_entry = self._create_label_entry(gsf, "Update Interval (ms):", r, self.update_interval_var, f"Min {MIN_UPDATE_INTERVAL_MS}ms."); r += 1
        self.max_plot_points_entry = self._create_label_entry(gsf, "Max Plot Points:", r, self.max_plot_points_var, f"Min {MIN_PLOT_POINTS}."); r += 1

        ttk.Label(gsf, text="Simulation Mode:").grid(row=r, column=0, **g)
        self.simulation_mode_check = ttk.Checkbutton(gsf, variable=self.simulation_mode_var, command=self.update_ui_state) # Update UI on change
        self.simulation_mode_check.grid(row=r, column=1, **g); r += 1

        ttk.Label(gsf, text="Stop Condition:").grid(row=r, column=0, **g)
        f = ttk.Frame(gsf); f.grid(row=r, column=1, **g)
        self.radio_stop_below = ttk.Radiobutton(f, text="Below Threshold", variable=self.stop_condition_var, value="below")
        self.radio_stop_below.pack(side=tk.LEFT, padx=5)
        self.radio_stop_above = ttk.Radiobutton(f, text="Above Threshold", variable=self.stop_condition_var, value="above")
        self.radio_stop_above.pack(side=tk.LEFT, padx=5); r += 1
        gsf.columnconfigure(1, weight=1)

        af = ttk.LabelFrame(self.settings_tab, text="Appearance"); af.pack(fill=tk.X, padx=FRAME_PADDING_X, pady=(FRAME_PADDING_Y_TOP, FRAME_PADDING_Y_BOTTOM))
        r = 0
        ttk.Label(af, text="GUI Theme:").grid(row=r, column=0, **g); f = ttk.Frame(af, **FRAME_STYLE); f.grid(row=r, column=1, **ge)
        themes = sorted(self.root.get_themes()) if hasattr(self.root, 'get_themes') else ["default"]
        self.gui_theme_combobox = ttk.Combobox(f, textvariable=self.gui_theme_var, state="readonly", values=themes)
        self.gui_theme_combobox.pack(fill=tk.X, expand=True); self.gui_theme_combobox.bind("<<ComboboxSelected>>", self.on_gui_theme_selected); r += 1
        ttk.Label(af, text="Plot Style:").grid(row=r, column=0, **g); f = ttk.Frame(af, **FRAME_STYLE); f.grid(row=r, column=1, **ge)
        self.plot_style_combobox = ttk.Combobox(f, textvariable=self.plot_style_var, state="readonly", values=sorted(plt.style.available))
        self.plot_style_combobox.pack(fill=tk.X, expand=True); self.plot_style_combobox.bind("<<ComboboxSelected>>", self.on_plot_style_selected); r += 1
        af.columnconfigure(1, weight=1)

        pf = ttk.LabelFrame(self.settings_tab, text="Presets"); pf.pack(fill=tk.BOTH, expand=True, padx=FRAME_PADDING_X, pady=(FRAME_PADDING_Y_TOP, UI_PADDING_Y))
        r = 0
        self.preset_name_entry = self._create_label_entry(pf, "Preset Name:", r, self.preset_name_var, "Enter name to save/load"); r += 1
        f = ttk.Frame(pf); f.grid(row=r, column=0, columnspan=2, pady=5, sticky=GRID_STICKY_EW)
        self.save_preset_button = ttk.Button(f, text="Save", command=self.save_preset, width=8); self.save_preset_button.pack(side=tk.LEFT, padx=5)
        self.load_preset_button = ttk.Button(f, text="Load", command=self.load_preset, width=8); self.load_preset_button.pack(side=tk.LEFT, padx=5)
        self.delete_preset_button = ttk.Button(f, text="Delete", command=self.delete_preset, width=8); self.delete_preset_button.pack(side=tk.LEFT, padx=5); r += 1
        ttk.Label(pf, text="Available:").grid(row=r, column=0, sticky=tk.NW, **p); f = ttk.Frame(pf, **FRAME_STYLE); f.grid(row=r, column=1, sticky=tk.NSEW, **p)
        self.preset_listbox = tk.Listbox(f, height=6); self.preset_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        s = ttk.Scrollbar(f, command=self.preset_listbox.yview); s.pack(side=tk.RIGHT, fill=tk.Y); self.preset_listbox.config(yscrollcommand=s.set); self.preset_listbox.bind("<Double-Button-1>", lambda e: self.load_preset()); r += 1
        self.load_presets_listbox(); pf.columnconfigure(1, weight=1); pf.rowconfigure(r-1, weight=1)

    def _setup_log_tab(self):
        """Builds the Log Tab UI."""
        f = ttk.Frame(self.log_tab); f.pack(fill=tk.BOTH, expand=True, padx=UI_PADDING_X, pady=(UI_PADDING_Y, FRAME_PADDING_Y_BOTTOM))
        self.log_text = tk.Text(f, state=tk.DISABLED, wrap=tk.WORD, height=10); self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        s = ttk.Scrollbar(f, command=self.log_text.yview); s.pack(side=tk.RIGHT, fill=tk.Y); self.log_text.config(yscrollcommand=s.set)
        # Define tag colors
        self.log_text.tag_config(STATUS_INFO, foreground="blue"); self.log_text.tag_config(STATUS_SUCCESS, foreground="green")
        self.log_text.tag_config(STATUS_WARNING, foreground="dark orange"); self.log_text.tag_config(STATUS_ERROR, foreground="red")
        self.log_text.tag_config(STATUS_DEBUG, foreground="gray")
        f_btn = ttk.Frame(self.log_tab); f_btn.pack(fill=tk.X, pady=FRAME_PADDING_Y_BOTTOM, padx=UI_PADDING_X)
        ttk.Button(f_btn, text="Clear Log", command=self.clear_log).pack(side=tk.LEFT)

    def _setup_notes_tab(self):
        """Builds the Notes Tab UI."""
        f = ttk.Frame(self.notes_tab); f.pack(fill=tk.BOTH, expand=True, padx=UI_PADDING_X, pady=(UI_PADDING_Y, FRAME_PADDING_Y_BOTTOM))
        self.notes_text = tk.Text(f, wrap=tk.WORD, height=10); self.notes_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        s = ttk.Scrollbar(f, command=self.notes_text.yview); s.pack(side=tk.RIGHT, fill=tk.Y); self.notes_text.config(yscrollcommand=s.set)
        f_btn = ttk.Frame(self.notes_tab); f_btn.pack(fill=tk.X, pady=FRAME_PADDING_Y_BOTTOM, padx=UI_PADDING_X)
        ttk.Button(f_btn, text="Save Notes", command=self.save_notes).pack(side=tk.LEFT, padx=5)

    def save_notes(self):
        """Saves notes from the text widget to config (but not file)."""
        if self.notes_text:
            self.config_manager.save_notes(self.notes_text.get("1.0", tk.END).strip())
            self.add_status_message("Notes updated (saved with config).", STATUS_INFO)

    def load_notes(self):
        """Loads notes from config into the text widget."""
        if self.notes_text:
            self.notes_text.config(state=tk.NORMAL)
            self.notes_text.delete("1.0", tk.END)
            self.notes_text.insert("1.0", self.config_manager.get_notes())

    
    def _update_threshold_label(self):
        """Updates threshold label and tooltips based on operation mode."""
        if not self.root.winfo_exists():
            return
        mode = self.operation_mode_var.get()
        if mode == MODE_CONSTANT_CURRENT:
            self.threshold_label_widget.config(text="Voltage Limit (V):")
            self.threshold_tooltip.set_text("CC: Stop when V > this limit.")
            self.voltage_tooltip.set_text("CC: OVP Level.")
            self.current_tooltip.set_text("CC: Target Current.")
            self.stop_condition_var.set("above")
        else:  # CV
            self.threshold_label_widget.config(text="Stop Threshold (A):")
            self.threshold_tooltip.set_text("CV: Stop when I < this limit.")
            self.voltage_tooltip.set_text("CV: Target Voltage.")
            self.current_tooltip.set_text("CV: OCP Level.")
            self.stop_condition_var.set("below")
        if not self.root.winfo_exists(): return
        mode = self.operation_mode_var.get()
        if mode == MODE_CONSTANT_CURRENT:
            self.threshold_label_widget.config(text="Voltage Limit (V):")
            self.threshold_tooltip.set_text("CC: Stop when V > this limit.")
            self.voltage_tooltip.set_text("CC: OVP Level.")
            self.current_tooltip.set_text("CC: Target Current.")
        else: # CV
            self.threshold_label_widget.config(text="Stop Threshold (A):")
            self.threshold_tooltip.set_text("CV: Stop when I < this limit.")
            self.voltage_tooltip.set_text("CV: Target Voltage.")
            self.current_tooltip.set_text("CV: OCP Level.")

    def scan_visa_resources(self, auto_scan=False):
        """Scans for VISA resources in a separate thread."""
        if not pyvisa:
            if not auto_scan: self.add_status_message("PyVISA not available.", STATUS_ERROR)
            if self.resource_combobox: self.resource_combobox['values'] = ["PyVISA Missing"]
            return
        if not auto_scan: self.add_status_message("Scanning VISA resources...", STATUS_INFO)
        if self.scan_button: self.scan_button.config(state=tk.DISABLED)
        if self.resource_combobox: self.resource_combobox.config(state=tk.DISABLED)
        self.executor.submit(self._scan_visa_task, auto_scan)

    def _scan_visa_task(self, auto_scan):
        """Task to list VISA resources."""
        resources, error_msg = [], None
        try:
            rm = pyvisa.ResourceManager()
            resources = rm.list_resources()
            rm.close()
        except Exception as e:
            error_msg = f"VISA Scan Error: {e}"
            print(traceback.format_exc())
        if self.root.winfo_exists():
            self.root.after(0, self._update_visa_list, resources, error_msg, auto_scan)

    def _update_visa_list(self, resources, error_msg, auto_scan):
        """Updates the VISA combobox in the GUI thread."""
        if not self.root.winfo_exists() or not self.resource_combobox: return

        current_value = self.resource_var.get()

        if error_msg:
            self.add_status_message(error_msg, STATUS_ERROR)
            self.resource_combobox['values'] = ["Scan Error"]
        elif resources:
            self.resource_combobox['values'] = list(resources)
            if not auto_scan or not current_value:
                 self.add_status_message(f"Found {len(resources)} VISA resource(s).", STATUS_INFO)
            # Try to re-select or select first if empty/not present
            if current_value in resources: self.resource_var.set(current_value)
            elif resources: self.resource_var.set(resources[0])
        else:
            self.resource_combobox['values'] = ["No Resources Found"]
            if not auto_scan: self.add_status_message("No VISA resources found.", STATUS_WARNING)

        if self.scan_button: self.scan_button.config(state=tk.NORMAL)
        self.update_ui_state() # Re-apply state based on sim mode etc.

    # --- Plotting Methods ---
    def _setup_plot_tab(self):
        """Builds the Plot Tab UI."""
        ttk.Button(self.plot_tab, text="Save Plot Image", command=self.save_plot_image).pack(side=tk.BOTTOM, pady=5)

    def _create_plot_canvas(self):
        if self.fig and self.plot_tab:
            # Destroy existing canvas widget if it exists
            # self.canvas is the FigureCanvasTkAgg instance
            if hasattr(self, 'canvas') and self.canvas and \
               hasattr(self.canvas, 'get_tk_widget') and \
               self.canvas.get_tk_widget().winfo_exists():
                self.canvas.get_tk_widget().destroy()

            self.canvas = FigureCanvasTkAgg(self.fig, master=self.plot_tab)
            self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    def _create_plot_toolbar(self):
        if self.canvas and self.plot_tab: # self.canvas must exist here
            # Destroy existing toolbar widget instance if it exists
            if hasattr(self, 'toolbar_instance') and self.toolbar_instance and \
               self.toolbar_instance.winfo_exists():
                self.toolbar_instance.destroy() # Destroy the old toolbar frame

            # Create and store the new toolbar instance
            self.toolbar_instance = NavigationToolbar2Tk(self.canvas, self.plot_tab)
            # Pack the new toolbar frame
            self.toolbar_instance.pack(side=tk.BOTTOM, fill=tk.X)
    def save_plot_image(self):
        """Saves the current plot to an image file."""
        if not self.fig:
            self.add_status_message("No plot to save.", STATUS_WARNING); return
        try:
            filepath = filedialog.asksaveasfilename(
                title="Save Plot", defaultextension=".png",
                filetypes=[("PNG", "*.png"), ("JPEG", "*.jpg"), ("PDF", "*.pdf")]
            )
            if filepath:
                self.fig.savefig(filepath)
                self.add_status_message(f"Plot saved: {os.path.basename(filepath)}", STATUS_SUCCESS)
        except Exception as e:
            self.add_status_message(f"Error saving plot: {e}", STATUS_ERROR)
            print(traceback.format_exc())

    def _create_plot_figure(self):
        """Creates the Matplotlib figure and axes."""
        if hasattr(self, 'fig') and self.fig: plt.close(self.fig)
        style = self.config_manager.config.get(CONFIG_PLOT_STYLE, DEFAULT_PLOT_STYLE)
        try:
            plt.style.use(style)
        except Exception as e:
            self.add_status_message(f"Warning: Plot style '{style}' error: {e}. Using default.", STATUS_WARNING)
            plt.style.use(DEFAULT_PLOT_STYLE)
        self.fig, axes = plt.subplots(4, 1, sharex=True, figsize=PLOT_FIGURE_SIZE)
        self.ax_v, self.ax_i, self.ax_p, self.ax_r = axes
        self.ax_v.set_ylabel("Voltage (V)"); self.ax_i.set_ylabel("Current (A)")
        self.ax_p.set_ylabel("Power (W)"); self.ax_r.set_ylabel("Resistance (Ω)")
        self.ax_r.set_xlabel("Time (s)")
        for ax in axes: ax.grid(True, linestyle=':', alpha=0.7)
        self.fig.tight_layout(pad=PLOT_PAD_INCHES, rect=PLOT_RECT_MARGIN)

    def _create_plot_lines(self):
        """Creates the Matplotlib line objects for plotting."""
        self.line_v, = self.ax_v.plot([], [], VOLTAGE_LINE_STYLE, label="V", lw=LINE_WIDTH)
        self.line_i, = self.ax_i.plot([], [], CURRENT_LINE_STYLE, label="I", lw=LINE_WIDTH)
        self.line_p, = self.ax_p.plot([], [], POWER_LINE_STYLE, label="P", lw=LINE_WIDTH)
        self.line_r, = self.ax_r.plot([], [], RESISTANCE_LINE_STYLE, label="R", lw=LINE_WIDTH)
        for ax in [self.ax_v, self.ax_i, self.ax_p, self.ax_r]:
            ax.legend(loc=LEGEND_LOCATION, fontsize=LEGEND_FONTSIZE)

    def _apply_plot_style(self, style_name: str):
        """Applies a new plot style and redraws."""
        self.config_manager.config[CONFIG_PLOT_STYLE] = style_name
        self._create_plot_figure(); self._create_plot_lines()
        self._create_plot_canvas(); self._create_plot_toolbar()
        if self.canvas: self.canvas.draw_idle()
        if self.is_logging and self.ani:
            self.ani.event_source.stop(); self.start_animation()

    def _apply_theme(self, theme_name: str):
        """Applies a new GUI theme."""
        if hasattr(self.root, 'set_theme'):
             self.root.set_theme(theme_name)
             self.add_status_message(f"Theme set to '{theme_name}'.", STATUS_INFO)
             self._redraw_plot_for_theme()

    def _redraw_plot_for_theme(self):
        """Redraws plot when theme changes to match colors."""
        self._apply_plot_style(self.config_manager.config.get(CONFIG_PLOT_STYLE))

    def on_gui_theme_selected(self, event=None):
        self._apply_theme(self.gui_theme_var.get())
        self.apply_and_save_config() # Save theme change

    def on_plot_style_selected(self, event=None):
        self._apply_plot_style(self.plot_style_var.get())
        self.apply_and_save_config() # Save style change

    def update_ui_state(self, logging_active: Optional[bool] = None):
        """Updates UI element states based on logging status or sim mode."""
        if logging_active is not None:
            self.is_logging = logging_active

        state = tk.DISABLED if self.is_logging else tk.NORMAL
        read_only = tk.DISABLED if self.is_logging else "readonly"
        sim_mode = self.simulation_mode_var.get()

        widgets_state = [
            (self.voltage_entry, state), (self.current_entry, state),
            (self.threshold_entry, state), (self.save_location_entry, state),
            (self.browse_button, state), (self.update_interval_entry, state),
            (self.max_plot_points_entry, state), (self.preset_name_entry, state),
            (self.save_preset_button, state), (self.delete_preset_button, state),
            (self.load_preset_button, state), (self.radio_stop_below, state),
            (self.radio_stop_above, state), (self.simulation_mode_check, state),
            (self.gui_theme_combobox, read_only), (self.plot_style_combobox, read_only),
            (self.preset_listbox, state),
            (self.start_button, tk.DISABLED if self.is_logging else tk.NORMAL),
            (self.stop_button, tk.NORMAL if self.is_logging else tk.DISABLED),
            (self.scan_button, tk.DISABLED if self.is_logging or sim_mode or not pyvisa else tk.NORMAL),
            (self.resource_combobox, tk.DISABLED if self.is_logging or sim_mode or not pyvisa else "readonly")
        ]
        all_radios = self.export_format_radios + self.operation_mode_radios
        for w in all_radios: widgets_state.append((w, state))

        for widget, st in widgets_state:
            if widget and widget.winfo_exists():
                try: widget.config(state=st)
                except tk.TclError: pass # Ignore if widget is mid-destruction

        if self.notes_text:
            try: self.notes_text.config(state=tk.DISABLED if self.is_logging else tk.NORMAL)
            except tk.TclError: pass

    def browse_save_location(self):
        """Opens a dialog to choose the save directory."""
        idir = self.save_location_var.get()
        if not os.path.isdir(idir): idir = self.config_manager.base_path
        sl = filedialog.askdirectory(title="Select Save Location", initialdir=idir)
        if sl: self.save_location_var.set(sl)

    def apply_and_save_config(self) -> bool:
        """Validates inputs, applies them, and saves config."""
        try:
            cfg = self.config_manager.config
            cfg[CONFIG_VOLTAGE] = validate_float_input(self.voltage_var.get(), "Voltage")
            cfg[CONFIG_CURRENT] = validate_float_input(self.current_var.get(), "Current")
            cfg[CONFIG_THRESHOLD] = validate_float_input(self.threshold_var.get(), "Threshold", allow_zero=True)
            cfg[CONFIG_UPDATE_INTERVAL] = validate_int_input(self.update_interval_var.get(), "Update Interval", MIN_UPDATE_INTERVAL_MS)
            cfg[CONFIG_MAX_PLOT_POINTS] = validate_int_input(self.max_plot_points_var.get(), "Max Plot Points", MIN_PLOT_POINTS)

            cfg[CONFIG_RESOURCE_NAME] = self.resource_var.get().strip()
            cfg[CONFIG_STOP_CONDITION] = self.stop_condition_var.get()
            save_location = self.save_location_var.get().strip()
            if not save_location:
                pass
            cfg[CONFIG_SAVE_LOCATION] = save_location
            cfg[CONFIG_EXPORT_FORMAT] = self.export_format_var.get()
            cfg[CONFIG_GUI_THEME] = self.gui_theme_var.get()
            cfg[CONFIG_PLOT_STYLE] = self.plot_style_var.get()
            cfg[CONFIG_SIMULATION_MODE] = self.simulation_mode_var.get()
            cfg[CONFIG_OPERATION_MODE] = self.operation_mode_var.get()
            cfg[CONFIG_ANODE] = self.anode_var.get().strip()
            cfg[CONFIG_CATHODE] = self.cathode_var.get().strip()
            cfg[CONFIG_ELECTROLYTE] = self.electrolyte_var.get().strip()
            cfg[CONFIG_ELECTROLYTE_MOLARITY] = self.electrolyte_molarity_var.get().strip()

            self.save_notes() # Ensure notes are in config before saving
            self.config_manager.save_config()
            self.data_manager = DataManager(cfg[CONFIG_MAX_PLOT_POINTS]) # Re-init if changed
            self.add_status_message("Settings applied and saved.", STATUS_SUCCESS)
            return True
        except ValueError as e:
            messagebox.showerror("Input Error", str(e))
            return False
        except Exception as e:
            messagebox.showerror("Config Error", f"Failed to save config: {e}")
            print(traceback.format_exc())
            return False

    def save_preset(self):
        """Saves current settings as a named preset."""
        name = self.preset_name_var.get().strip()
        if not name: messagebox.showwarning("Input Error", "Please enter a preset name."); return
        try:
            # First, validate current inputs before saving
            v = validate_float_input(self.voltage_var.get(), "Voltage")
            c = validate_float_input(self.current_var.get(), "Current")
            t = validate_float_input(self.threshold_var.get(), "Threshold", allow_zero=True)
            p_data = {
                CONFIG_RESOURCE_NAME: self.resource_var.get(), CONFIG_VOLTAGE: v,
                CONFIG_CURRENT: c, CONFIG_THRESHOLD: t,
                CONFIG_STOP_CONDITION: self.stop_condition_var.get(),
                CONFIG_EXPORT_FORMAT: self.export_format_var.get(),
                CONFIG_OPERATION_MODE: self.operation_mode_var.get(),
                CONFIG_ANODE: self.anode_var.get(), CONFIG_CATHODE: self.cathode_var.get(),
                CONFIG_ELECTROLYTE: self.electrolyte_var.get(),
                CONFIG_ELECTROLYTE_MOLARITY: self.electrolyte_molarity_var.get()
            }
            if self.config_manager.add_preset(name, p_data):
                self.load_presets_listbox()
                self.add_status_message(f"Preset '{name}' saved successfully.", STATUS_SUCCESS)
        except ValueError as e: messagebox.showerror("Input Error", f"Cannot save preset due to invalid input:\n{e}")
        except Exception as e: self.add_status_message(f"Error saving preset: {e}", STATUS_ERROR)

    def load_presets_listbox(self):
        """Reloads the preset listbox from the config manager."""
        if self.preset_listbox:
            self.preset_listbox.delete(0, tk.END)
            for name in self.config_manager.get_preset_names():
                self.preset_listbox.insert(tk.END, name)
            self.update_ui_state()

    def load_preset(self):
        """Loads selected preset into the UI fields."""
        if not self.preset_listbox: return
        try:
            sel = self.preset_listbox.curselection()
            if not sel: messagebox.showinfo("Selection Required", "Please select a preset to load."); return
            name = self.preset_listbox.get(sel[0])
            preset = self.config_manager.get_preset(name)
            if preset:
                self.resource_var.set(preset.get(CONFIG_RESOURCE_NAME, ""))
                self.voltage_var.set(str(preset.get(CONFIG_VOLTAGE, 4.0)))
                self.current_var.set(str(preset.get(CONFIG_CURRENT, 0.5)))
                self.threshold_var.set(str(preset.get(CONFIG_THRESHOLD, 0.062)))
                self.stop_condition_var.set(preset.get(CONFIG_STOP_CONDITION, "below"))
                self.export_format_var.set(preset.get(CONFIG_EXPORT_FORMAT, "csv"))
                self.operation_mode_var.set(preset.get(CONFIG_OPERATION_MODE, MODE_CONSTANT_VOLTAGE))
                self.anode_var.set(preset.get(CONFIG_ANODE, ""))
                self.cathode_var.set(preset.get(CONFIG_CATHODE, ""))
                self.electrolyte_var.set(preset.get(CONFIG_ELECTROLYTE, ""))
                self.electrolyte_molarity_var.set(preset.get(CONFIG_ELECTROLYTE_MOLARITY, ""))
                self.preset_name_var.set(name)
                self._update_threshold_label()
                self.add_status_message(f"Preset '{name}' loaded successfully.", STATUS_SUCCESS)
        except Exception as e:
            self.add_status_message(f"Error loading preset: {e}", STATUS_ERROR)
            print(traceback.format_exc())

    def delete_preset(self):
        """Deletes the selected preset."""
        if not self.preset_listbox: return
        sel = self.preset_listbox.curselection()
        if not sel: messagebox.showinfo("Selection Required", "Please select a preset to delete."); return
        name = self.preset_listbox.get(sel[0])
        if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete preset '{name}'?"):
            if self.config_manager.delete_preset(name):
                self.load_presets_listbox()
                self.add_status_message(f"Preset '{name}' deleted.", STATUS_INFO)

    def add_status_message(self, message: str, level: str = STATUS_INFO):
        """Adds a message to the status bar and the log text widget."""
        ts = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{ts}] {message}\n"

        if self.log_text and self.log_text.winfo_exists():
            try:
                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, log_msg, level)
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
            except tk.TclError: pass # Ignore if widget gone

        if self.status_indicator and self.status_indicator.winfo_exists():
            try:
                self.status_var.set(message.split('\n')[0]) # Show first line
                color = {"info":"blue", "success":"green", "warning":"dark orange", "error":"red", "debug": "gray"}.get(level, "black")
                self.status_indicator.config(foreground=color)
            except tk.TclError: pass

    def clear_log(self):
        """Clears the log text widget."""
        if self.log_text:
            self.log_text.config(state=tk.NORMAL); self.log_text.delete(1.0, tk.END)
            self.log_text.config(state=tk.DISABLED); self.add_status_message("Log cleared.", STATUS_INFO)

    def start_logging(self):
        """Starts the data logging process."""
        if self.is_logging:
            messagebox.showwarning("Already Running", "Logging is already in progress."); return

        if not self.apply_and_save_config(): # Validate & Save before starting
            self.add_status_message("Cannot start logging due to configuration errors.", STATUS_ERROR)
            return

        cfg = self.config_manager.config
        if not cfg.get(CONFIG_SIMULATION_MODE) and not cfg.get(CONFIG_RESOURCE_NAME):
            messagebox.showerror("Error", "Please select a VISA Resource or enable Simulation Mode.")
            return

        self.data_manager.clear_plot_data() # Clear plot data
        self.logger = DataLogger(cfg, self.data_manager, self.status_queue)
        self.add_status_message("Initializing logger...", STATUS_INFO)
        self.update_ui_state(True)
        if self.notebook: self.notebook.select(self.plot_tab) # Switch to plot tab
        self.executor.submit(self._connect_and_start_logging_task)

    def _connect_and_start_logging_task(self):
        """Thread task to connect and start the logger."""
        if self.logger:
            if self.logger.connect():
                if self.logger.start():
                    self.root.after(0, self._post_logging_start_tasks)
                    return
        # If any step fails, call the failure handler
        self.root.after(0, self._handle_start_failure)

    def _post_logging_start_tasks(self):
        """Tasks to run in GUI thread after logger starts."""
        if self.root.winfo_exists():
            self.start_animation() # Start plot updates

    def _handle_start_failure(self):
        """Handles GUI updates when logging fails to start."""
        if self.root.winfo_exists():
            self.add_status_message("Logging failed to start. Check connection and settings.", STATUS_ERROR)
            if self.logger: self.logger.stop() # Ensure cleanup
            self.logger = None
            self.update_ui_state(False)

    def stop_logging(self):
        """Stops the data logging process."""
        if not self.is_logging: return
        self.add_status_message("Stopping logging...", STATUS_INFO)

        if self.ani and self.ani.event_source:
            try: self.ani.event_source.stop()
            except Exception: pass
            self.ani = None

        if self.logger:
            notes = self.notes_text.get("1.0", tk.END).strip() if self.notes_text else ""
            self.logger.stop(notes_content=notes)
            self.logger = None

        self.update_ui_state(False)

    def start_animation(self):
        """Starts the Matplotlib animation for live plotting."""
        if not self.fig or not self.canvas: return
        interval = self.config_manager.config.get(CONFIG_UPDATE_INTERVAL)
        self.ani = animation.FuncAnimation(self.fig, self._update_plot,
                                         interval=max(interval, MIN_UPDATE_INTERVAL_MS),
                                         blit=False, cache_frame_data=False, repeat=True)
        self.canvas.draw_idle()

    def _update_plot(self, frame):
        """Animation function to update plot data."""
        lines = [self.line_v, self.line_i, self.line_p, self.line_r]
        if not all(lines) or not self.is_logging or not self.data_manager:
            return tuple(l for l in lines if l)

        try:
            # Process one point to keep GUI responsive
            while not self.data_manager.empty():
                self.data_manager.append_for_plotting(*self.data_manager.get_nowait())
                break # Only process one per frame update

            if self.data_manager.time_data:
                t, v, i, p, r = (np.array(d) for d in [
                    self.data_manager.time_data, self.data_manager.voltage_data,
                    self.data_manager.current_data, self.data_manager.power_data,
                    self.data_manager.resistance_data])
                self.line_v.set_data(t, v)
                self.line_i.set_data(t, i)
                self.line_p.set_data(t, p)
                self.line_r.set_data(t, r)
                self._update_plot_axes()

        except queue.Empty:
            pass # No new data, just return
        except Exception as e:
            print(f"Debug: Plot Update Error: {e}")
            # print(traceback.format_exc()) # Uncomment for detailed debug

        return tuple(l for l in lines if l)

    def _update_plot_axes(self):
        """Updates the plot axes limits."""
        dm = self.data_manager
        axes = [self.ax_v, self.ax_i, self.ax_p, self.ax_r]
        datas = [dm.voltage_data, dm.current_data, dm.power_data, dm.resistance_data]
        t_min = 0.0
        t_max = dm.time_data[-1] if dm.time_data else 1.0 # Default to 1.0 if no data

        # Ensure t_max is always > t_min
        t_upper = max(t_max * 1.1, t_min + 1.0)
        self.ax_r.set_xlim(t_min, t_upper)

        for ax, data in zip(axes, datas):
            if not data: ax.set_ylim(0, 1); continue
            d_finite = [x for x in data if np.isfinite(x)]
            if not d_finite: y_min, y_max = 0.0, 1.0
            else: y_min, y_max = min(d_finite), max(d_finite)
            y_range = y_max - y_min
            pad = 0.1 if abs(y_range) < 1e-6 else 0.05 * y_range
            ax.set_ylim(y_min - pad, y_max + pad)

        # No need to call draw_idle here, FuncAnimation handles it.

    def check_queues(self):
        """Periodically checks status/error queues."""
        try:
            while not self.status_queue.empty():
                level, message = self.status_queue.get_nowait()
                self.add_status_message(message, level)
                # Check for special signals
                if level == "STOP_SIGNAL": self.stop_logging()
                if level == "LOGGER_FINISHED" and self.is_logging: self.stop_logging()

        except queue.Empty:
            pass
        except Exception as e:
            print(f"Error checking queues: {e}")

        self._after_check_queues_id = self.root.after(150, self.check_queues) # Poll ~6-7 times/sec

    def on_closing(self):
        """Handles window closing event."""
        if self._after_check_queues_id:
            try: self.root.after_cancel(self._after_check_queues_id)
            except Exception: pass

        if self.is_logging:
            if not messagebox.askyesno("Confirm Exit", "Logging is active. Are you sure you want to stop and exit?"):
                return

        self.add_status_message("Exiting application...", STATUS_INFO)
        self.stop_logging() # Ensure logging is stopped & data saved
        self.apply_and_save_config() # Save final settings
        plt.close('all') # Close plot windows
        self.executor.shutdown(wait=False, cancel_futures=True) # Don't wait for threads
        self.root.destroy()

# =============================================================================
# == Main Execution & Exception Handling ==
# =============================================================================

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Catches unhandled exceptions and displays them."""
    error_msg = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    print(f"--- UNHANDLED EXCEPTION ---\n{error_msg}---------------------------\n")
    try:
        # Try to show a messagebox if Tkinter is available
        messagebox.showerror("Unhandled Application Error",
                             "A critical error occurred. Please check the "
                             "console output or log files for details.\n\n"
                             f"{exc_type.__name__}: {exc_value}")
    except Exception as e:
        print(f"Could not display unhandled exception in messagebox: {e}")

def main():
    """Sets up the global exception handler and runs the Tkinter app."""
    sys.excepthook = global_exception_handler
    root = None
    try:
        root = ThemedTk()
        app = PowerLoggerApp(root)
        root.mainloop()
    except Exception as e:
        # This catches errors during Tkinter/App setup.
        # The hook should have already caught/printed it.
        print(f"Fatal error during main execution: {e}")
        if root and root.winfo_exists():
            try: root.destroy()
            except Exception: pass
        sys.exit(1)

if __name__ == "__main__":
    main()
