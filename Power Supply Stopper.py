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

# --- Third-Party Libraries ---
try:
    import winsound
except ImportError:
    # Allow operation on non-Windows systems
    winsound = None

try:
    import pyvisa
except ImportError:
    messagebox.showerror(
        "Missing Dependency",
        "PyVISA is not installed. Please install it (`pip install pyvisa`) "
        "to communicate with the instrument.\n\n"
        "Simulation mode can still be used.",
    )
    pyvisa = None # Allow running in simulation mode without pyvisa

try:
    import pandas as pd
except ImportError:
    # Handle missing pandas - disable Excel/JSON/All export
    pd = None

try:
    import openpyxl
except ImportError:
    # openpyxl is needed by pandas for '.xlsx'
    openpyxl = None # Not strictly necessary to check, pandas handles it

try:
    import numpy as np
except ImportError:
    messagebox.showerror(
        "Missing Dependency",
        "NumPy is not installed. Please install it (`pip install numpy`)."
    )
    sys.exit(1) # NumPy is critical for plotting

try:
    import matplotlib
    matplotlib.use('TkAgg') # Ensure TkAgg backend is used
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    from matplotlib.figure import Figure
    from matplotlib.axes import Axes
    from matplotlib.lines import Line2D
except ImportError:
     messagebox.showerror(
        "Missing Dependency",
        "Matplotlib is not installed. Please install it (`pip install matplotlib`)."
     )
     sys.exit(1) # Matplotlib is critical

try:
    from ttkthemes import ThemedTk
except ImportError:
    messagebox.showwarning(
        "Missing Dependency",
        "ttkthemes not found (`pip install ttkthemes`). Falling back to default TTK styles."
    )
    # Fallback to standard Tkinter if ttkthemes is missing
    class ThemedTk(tk.Tk):
        def set_theme(self, theme_name):
            print(f"ttkthemes not installed, cannot set theme '{theme_name}'. Using default.")
        def get_themes(self):
            # Return a dummy list if themes aren't available
            return ["default (ttkthemes not installed)"]
try:
    from PIL import Image, ImageTk
except ImportError:
    # Handle cases where Pillow is not installed
    Image = None
    ImageTk = None
    print("Warning: Pillow not installed. Cannot use iconphoto for advanced image types.")

# --- Constants ---
DEFAULT_CONFIG_FILENAME = "power_logger_config.json"
DEFAULT_PRESETS_FILENAME = "power_logger_presets.json"
DEFAULT_GUI_THEME = "clam"
DEFAULT_PLOT_STYLE = "default"
DEFAULT_EXPORT_FORMAT = "csv"
VALID_EXPORT_FORMATS = ["csv", "xlsx", "json", "all"]
STATUS_INFO = "info"
STATUS_SUCCESS = "success"
STATUS_WARNING = "warning"
STATUS_ERROR = "error"

# --- New Constants for Improvements ---
# Fade Effect Settings
DEFAULT_ENABLE_THEME_FADE = True
FADE_STEPS = 10
FADE_DURATION_MS = 2000 # Original duration
FAST_FADE_STEPS = 5
FAST_FADE_DURATION_MS = 100 # Faster duration

# Minimum Intervals
MIN_UPDATE_INTERVAL_MS = 10 # Minimum for data acquisition and plot update
MIN_PLOT_POINTS = 10 # Minimum data points to show on plot

# Plotting Settings
PLOT_FIGURE_SIZE = (8, 7) # Default figure size (width, height in inches)
PLOT_PAD_INCHES = 1.5 # Padding around plot
PLOT_RECT_MARGIN = [0, 0.03, 1, 0.97] # [left, bottom, right, top] as a fraction of figure width/height

# UI Padding
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

# Status Bar Settings
STATUS_WRAPLENGTH = 800 # Wrap status messages after this many pixels

# Plot Line Styles (can be moved to config or settings later)
VOLTAGE_LINE_STYLE = 'b-'
CURRENT_LINE_STYLE = 'r-'
POWER_LINE_STYLE = 'g-'
RESISTANCE_LINE_STYLE = 'c-'
LINE_WIDTH = 1.5
LEGEND_LOCATION = 'upper right'
LEGEND_FONTSIZE = 'small'

# Data Logger Settings
VISA_OPEN_TIMEOUT_MS = 5000
VISA_READ_WRITE_TIMEOUT_MS = 5000
INITIAL_SETTLING_TIME_S = 1.0 # Time before checking stop condition

# --- Constants for Configuration Keys ---
CONFIG_RESOURCE_NAME = "RESOURCE_NAME"
CONFIG_VOLTAGE = "VOLTAGE"
CONFIG_CURRENT = "CURRENT"
CONFIG_THRESHOLD = "THRESHOLD"
CONFIG_STOP_CONDITION = "STOP_CONDITION"
CONFIG_UPDATE_INTERVAL = "UPDATE_INTERVAL"
CONFIG_MAX_PLOT_POINTS = "MAX_PLOT_POINTS"
CONFIG_SAVE_LOCATION = "SAVE_LOCATION"
CONFIG_EXPORT_FORMAT = "EXPORT_FORMAT"
CONFIG_GUI_THEME = "GUI_THEME"
CONFIG_PLOT_STYLE = "PLOT_STYLE"
CONFIG_SIMULATION_MODE = "SIMULATION_MODE"
CONFIG_NOTES = "NOTES"
CONFIG_ENABLE_THEME_FADE = "ENABLE_THEME_FADE"
CONFIG_ANODE = "ANODE"
CONFIG_CATHODE = "CATHODE"
CONFIG_ELECTROLYTE = "ELECTROLYTE"
CONFIG_ELECTROLYTE_MOLARITY = "ELECTROLYTE_MOLARITY (M)"
CONFIG_OPERATION_MODE = "OPERATION_MODE" # New

# --- Operation Modes ---
MODE_CONSTANT_VOLTAGE = "Constant Voltage" # New
MODE_CONSTANT_CURRENT = "Constant Current"   # New


# --- Tooltip Class ---
class Tooltip:
    """Helper class to display tooltips for Tkinter widgets."""
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.widget.bind("<Enter>", self.show_tooltip)
        self.widget.bind("<Leave>", self.hide_tooltip)
        # Bind destroy to hide tooltip if widget is destroyed
        self.widget.bind("<Destroy>", self.hide_tooltip)


    def show_tooltip(self, event=None):
        """Displays the tooltip window."""
        if self.tooltip_window or not self.text or not self.widget.winfo_exists():
            return # Don't show if already showing, no text, or widget is gone
        try:
            x, y, _, _ = self.widget.bbox("insert")
            x = x + self.widget.winfo_rootx() + 25
            y = y + self.widget.winfo_rooty() + 20

            self.tooltip_window = tk.Toplevel(self.widget)
            self.tooltip_window.wm_overrideredirect(True) # Hide window decorations
            self.tooltip_window.wm_geometry(f"+{x}+{y}")

            label = ttk.Label(self.tooltip_window, text=self.text, background="#FFFFEA", relief=tk.SOLID, borderwidth=1,
                              wraplength=300) # Added wraplength
            label.pack(ipadx=1)
        except tk.TclError:
            # Handle cases where widget geometry or winfo_exists() fails during shutdown
            self.hide_tooltip() # Attempt to hide any partially created window
        except Exception as e:
            print(f"Error showing tooltip: {e}")
            self.hide_tooltip()


    def hide_tooltip(self, event=None):
        """Hides the tooltip window."""
        if self.tooltip_window:
            try:
                self.tooltip_window.destroy()
            except tk.TclError:
                 pass # Ignore if window already destroyed
            self.tooltip_window = None

# --- Configuration Management ---
class ConfigManager:
    """Handles loading, saving, and managing application configuration and presets."""

    def __init__(self, config_file: str = DEFAULT_CONFIG_FILENAME, presets_file: str = DEFAULT_PRESETS_FILENAME):
        """
         Initializes the ConfigManager.

        Args:
            config_file: The filename for the main configuration.
            presets_file: The filename for presets.
        """
        # Get the directory where the script is located
        self.base_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        self.config_file: str = os.path.join(self.base_path, config_file)
        self.presets_file: str = os.path.join(self.base_path, presets_file)

        # Default configuration using constants
        self.config: Dict[str, Any] = {
            CONFIG_RESOURCE_NAME: "USB0::0x1AB1::0x0E11::DP8D154300049::INSTR", # Example Rigol ID
            CONFIG_VOLTAGE: 4.0,  # Volts
            CONFIG_CURRENT: 0.5,  # Amps
            CONFIG_THRESHOLD: 0.062,  # Amps
            CONFIG_STOP_CONDITION: "Below", # New setting: "below" or "above"
            CONFIG_UPDATE_INTERVAL: 200,  # Update plot every 200ms
            CONFIG_MAX_PLOT_POINTS: 1000,  # Maximum data points to show on the plot
            CONFIG_SAVE_LOCATION: self.base_path, # Default to script directory
            CONFIG_EXPORT_FORMAT: DEFAULT_EXPORT_FORMAT,
            CONFIG_GUI_THEME: DEFAULT_GUI_THEME,
            CONFIG_PLOT_STYLE: DEFAULT_PLOT_STYLE,
            CONFIG_SIMULATION_MODE: False,
            CONFIG_NOTES: "", # Add a key for notes, default to empty string
            CONFIG_ENABLE_THEME_FADE : DEFAULT_ENABLE_THEME_FADE, # Add this new setting using the constant
            CONFIG_ANODE: "",
            CONFIG_CATHODE: "",
            CONFIG_ELECTROLYTE: "",
            CONFIG_ELECTROLYTE_MOLARITY: "", # Store as string to allow "N/A"
            CONFIG_OPERATION_MODE: MODE_CONSTANT_VOLTAGE # New: Default operation mode
        }
        self.presets: Dict[str, Dict[str, Any]] = {}
        self.load_config()
        self.load_presets()

    def load_config(self) -> None:
        """Load main configuration from file, merging with defaults."""
        if not os.path.exists(self.config_file):
            print(f"Info: Config file '{self.config_file}' not found. Using defaults.")
            return

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f: # Use utf-8 encoding
                loaded_config = json.load(f)
                # Merge with defaults to handle new keys added in code
                # Existing keys in the file override defaults
                default_copy = self.config.copy()
                default_copy.update(loaded_config)
                self.config = default_copy
        except FileNotFoundError:
            print(f"Info: Config file '{self.config_file}' not found during load. Using defaults.")
        except json.JSONDecodeError as e:
            print(f"Warning: Error decoding configuration file {self.config_file}: {e}. Using defaults.")
        except Exception as e:
            print(f"Warning: Error loading configuration from {self.config_file}: {e}. Using defaults.")

    def save_config(self) -> None:
        """Save current main configuration to file."""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f: # Use utf-8 encoding
                json.dump(self.config, f, indent=4)
        except IOError as e:
            print(f"Error saving configuration to {self.config_file}: File I/O error - {e}")
        except Exception as e:
            print(f"Error saving configuration to {self.config_file}: {e}")

    def load_presets(self) -> None:
        """Load presets from file."""
        if not os.path.exists(self.presets_file):
            return # No presets file, that's okay

        try:
            with open(self.presets_file, 'r', encoding='utf-8') as f: # Use utf-8 encoding
                self.presets = json.load(f)
        except FileNotFoundError:
             print(f"Info: Presets file '{self.presets_file}' not found during load. No presets loaded.")
        except json.JSONDecodeError as e:
            print(f"Warning: Error decoding presets file {self.presets_file}: {e}. No presets loaded.")
            self.presets = {} # Ensure presets is empty list/dict on decode error
        except Exception as e:
            print(f"Warning: Error loading presets from {self.presets_file}: {e}. No presets loaded.")
            self.presets = {} # Ensure presets is empty list/dict on other errors

    def add_preset(self, name: str, preset_data: Dict[str, Any]) -> bool:
        """
        Add or update a preset.

        Args:
            name: The name of the preset.
            preset_data: The dictionary containing preset settings.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not name:
            print("Warning: Attempted to save preset with empty name.")
            return False
        # Sanitize preset_data to include only relevant config keys
        sanitized_data = {key: preset_data.get(key) for key in [
            CONFIG_RESOURCE_NAME, CONFIG_VOLTAGE, CONFIG_CURRENT,
            CONFIG_THRESHOLD, CONFIG_STOP_CONDITION, CONFIG_EXPORT_FORMAT,
            CONFIG_ANODE, CONFIG_CATHODE, CONFIG_ELECTROLYTE, CONFIG_ELECTROLYTE_MOLARITY,
            CONFIG_OPERATION_MODE # New: Include operation mode in presets
        ]}
        self.presets[name] = sanitized_data
        self.save_presets()
        return True

    def get_preset(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get preset data by name.

        Args:
            name: The name of the preset.

        Returns:
            Optional[Dict[str, Any]]: The preset data dictionary, or None if not found.
        """
        return self.presets.get(name)

    def delete_preset(self, name: str) -> bool:
        """
        Delete a preset by name.

        Args:
            name: The name of the preset.

        Returns:
            bool: True if deleted, False if not found.
        """
        if name in self.presets:
            del self.presets[name]
            self.save_presets()
            return True
        return False

    def get_preset_names(self) -> List[str]:
        """
        Get a sorted list of preset names.

        Returns:
            List[str]: Sorted list of preset names.
        """
        return sorted(self.presets.keys())

    def save_presets(self) -> None:
        """Save current presets to file."""
        try:
            with open(self.presets_file, 'w', encoding='utf-8') as f: # Use utf-8 encoding
                json.dump(self.presets, f, indent=4)
        except IOError as e:
            print(f"Error saving presets to {self.presets_file}: File I/O error - {e}")
        except Exception as e:
            print(f"Error saving presets to {self.presets_file}: {e}")

    # --- Notes Management ---
    def get_notes(self) -> str:
        """
        Get the current notes content from config.

        Returns:
            str: The notes content.
        """
        # Use constant for config key
        return self.config.get(CONFIG_NOTES, "")

    def save_notes(self, notes_content: str) -> None:
        """
        Save the notes content to the configuration.

        Args:
            notes_content: The content of the notes text field.
        """
        # Use constant for config key
        self.config[CONFIG_NOTES] = notes_content
        # Notes are saved as part of the main config save
        # self.save_config() # Don't save here, save happens on exit or apply settings


# --- Data Management ---
class DataManager:
    """Manages data points for plotting using efficient deques."""
    def __init__(self, max_plot_points: int = 1000):
        if max_plot_points < 2:
             print(f"Warning: MAX_PLOT_POINTS adjusted to minimum of 2 (was {max_plot_points})")
             max_plot_points = 2 # Need at least 2 points for plotting lines
        self.max_plot_points = max_plot_points
        self.data_queue: queue.Queue[List[float]] = queue.Queue()
        # Use deque for efficient sliding window
        self.time_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.voltage_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.current_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.power_data: Deque[float] = deque(maxlen=self.max_plot_points)
        self.resistance_data: Deque[float] = deque(maxlen=self.max_plot_points)

    def put(self, data_point: List[float]) -> None:
        """Put a data point (list: [time, V, I, P, R]) into the queue."""
        self.data_queue.put(data_point)

    def get_nowait(self) -> List[float]:
        """Get a data point from the queue without blocking."""
        return self.data_queue.get_nowait()

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self.data_queue.empty()

    def append_for_plotting(self, time_s: float, voltage: float, current: float, power: float, resistance: float) -> None:
        """Append data to deques for plotting (sliding window)."""
        self.time_data.append(time_s)
        self.voltage_data.append(voltage)
        self.current_data.append(current)
        self.power_data.append(power)
        self.resistance_data.append(resistance)

    def clear_plot_data(self) -> None:
        """Clear data used for plotting."""
        self.time_data.clear()
        self.voltage_data.clear()
        self.current_data.clear()
        self.power_data.clear()
        self.resistance_data.clear()

# --- Data Logger ---
class DataLogger:
    """
    Handles the data logging process separately from the GUI.
    Interacts with the instrument or generates simulated data.
    Stores data in memory and exports to chosen formats when stopped.
    """
    def __init__(self, config: Dict[str, Any], data_manager: DataManager,
                 status_queue: queue.Queue[Tuple[str, str]], error_queue: queue.Queue[Tuple[str, str]]):
        """
        Initialize the data logger.

        Args:
            config: Configuration dictionary.
            data_manager: The DataManager instance.
            status_queue: Queue for sending status messages to GUI.
            error_queue: Queue for sending error messages to GUI.
        """
        self.config = config
        self.data_manager = data_manager
        self.status_queue = status_queue
        self.error_queue = error_queue

        self.dp: Optional[pyvisa.Resource] = None # Instrument connection
        self.rm: Optional[pyvisa.ResourceManager] = None # Resource manager instance
        self.stop_event = threading.Event()
        self.logging_thread: Optional[threading.Thread] = None
        self.is_simulating: bool = self.config.get(CONFIG_SIMULATION_MODE, False)

        # In-memory storage for ALL logged data points
        self._logged_data: List[List[float]] = []
        # Define headers consistently
        self._data_headers = ["Time (s)", "Voltage (V)", "Current (A)", "Power (W)", "Resistance (Ω)"]


        # Simulation specific variables
        self._sim_current: float = self.config.get(CONFIG_CURRENT, 0.5)
        self._sim_voltage: float = self.config.get(CONFIG_VOLTAGE, 4.0)
        self._sim_start_time: Optional[float] = None

    def connect(self) -> bool:
        """
        Connect to the instrument or simulate connection.

        Returns:
            bool: True if connection is successful (real or simulated), False otherwise.
        """
        if self.is_simulating:
            self.status_queue.put((STATUS_INFO, "Simulation mode active. Skipping instrument connection."))
            return True

        # Assume pyvisa, STATUS_INFO, STATUS_ERROR, STATUS_SUCCESS are defined elsewhere
        if not pyvisa:
             self.error_queue.put((STATUS_ERROR, "PyVISA not available. Cannot connect to instrument."))
             return False

        # Assume CONFIG_RESOURCE_NAME is defined elsewhere
        resource_name = self.config.get(CONFIG_RESOURCE_NAME)
        if not resource_name:
            self.error_queue.put((STATUS_ERROR, "No VISA resource selected in configuration."))
            return False

        try:
            self.status_queue.put((STATUS_INFO, f"Attempting to connect to: {resource_name}"))
            self.rm = pyvisa.ResourceManager()
            # Use timeouts for connection and operations
            self.dp = self.rm.open_resource(resource_name, open_timeout=VISA_OPEN_TIMEOUT_MS)
            self.dp.timeout = VISA_READ_WRITE_TIMEOUT_MS
            self.dp.read_termination = '\n'
            self.dp.write_termination = '\n'
            idn = self.dp.query('*IDN?').strip()
            self.status_queue.put((STATUS_SUCCESS, f"Connected to: {idn}"))
            return True
        except pyvisa.errors.VisaIOError as e:
            self.error_queue.put((STATUS_ERROR, f"VISA IO Error during connection: {e}"))
            self._close_connection()
            return False
        except Exception as e:
            self.error_queue.put((STATUS_ERROR, f"Connection error: {e}"))
            # Assume traceback is imported
            traceback.print_exc() # Log full traceback for debugging
            self._close_connection()
            return False

    def _close_connection(self) -> None:
        """Safely close the instrument connection and resource manager."""
        if self.dp:
            try:
                # Ensure output is off before closing in case stop wasn't called properly
                # or if auto-stop happened and main thread didn't call stop yet
                if not self.is_simulating:
                    try:
                        self.dp.write(":OUTP OFF")
                    except Exception:
                        pass # Ignore errors turning off if connection already bad
                self.dp.close()
                # Assume STATUS_INFO is defined elsewhere
                self.status_queue.put((STATUS_INFO, "Instrument connection closed"))
            # Assume STATUS_ERROR is defined elsewhere
            except pyvisa.errors.VisaIOError as e:
                 self.error_queue.put((STATUS_ERROR, f"VISA IO Error during close: {e}"))
            except Exception as e:
                self.error_queue.put((STATUS_ERROR, f"Error closing instrument: {e}"))
            finally:
                 self.dp = None

        if self.rm:
            try:
                 self.rm.close()
                 # No message needed for resource manager close usually
            # Assume STATUS_ERROR is defined elsewhere
            except Exception as e:
                 self.error_queue.put((STATUS_ERROR, f"Error closing resource manager: {e}"))
            finally:
                 self.rm = None

    def start(self) -> bool:
        """
        Start the data logging process in a separate thread (real or simulated).

        Returns:
            bool: True if logging started successfully, False otherwise.
        """
        self.stop_event.clear() # Reset stop event

        # Assume STATUS_ERROR, STATUS_WARNING, STATUS_INFO are defined elsewhere
        if not self.is_simulating and not self.dp:
            self.error_queue.put((STATUS_ERROR, "Instrument not connected. Cannot start logging."))
            return False
        if self.logging_thread and self.logging_thread.is_alive():
            self.error_queue.put((STATUS_WARNING, "Logging thread already running."))
            return False

        try:
            # --- Setup Instrument or Simulation ---
            # Assume CONFIG_VOLTAGE, CONFIG_CURRENT are defined elsewhere
            voltage = self.config.get(CONFIG_VOLTAGE, 0.0) # Use .get with default for safety
            current = self.config.get(CONFIG_CURRENT, 0.0) # Use .get with default for safety
            operation_mode = self.config.get(CONFIG_OPERATION_MODE, MODE_CONSTANT_VOLTAGE) # Get mode

            if not self.is_simulating and self.dp:
                # Set up the power supply
                self.dp.write(f":APPL CH1,{voltage},{current}")
                self.dp.write(":OUTP CH1,ON") # Ensure correct channel if multi-channel
                self.status_queue.put((STATUS_INFO, f"Set CH1: {voltage}V, {current}A, Output ON. Mode: {operation_mode}"))
            else:
                 # Initialize simulation variables
                 self._sim_current = current
                 self._sim_voltage = voltage
                 # Assume time is imported
                 self._sim_start_time = time.time()
                 self.status_queue.put((STATUS_INFO, f"Simulation output ON (Virtual). Mode: {operation_mode}"))


            # --- Clear Old Data & Start Thread ---
            # Clear in-memory data from previous runs
            self._logged_data = []
            # Plot data is cleared by DataManager in start_logging (called before logger.start())

            self.logging_thread = threading.Thread(target=self._log_data, daemon=True)
            self.logging_thread.start()
            return True

        # Assume STATUS_ERROR, STATUS_WARNING are defined elsewhere
        except pyvisa.errors.VisaIOError as e:
            self.error_queue.put((STATUS_ERROR, f"VISA IO Error setting up instrument: {e}"))
            self.stop() # Attempt cleanup
            return False
        except Exception as e:
            self.error_queue.put((STATUS_ERROR, f"Failed to start logging: {e}"))
            # Assume traceback is imported
            traceback.print_exc()
            self.stop() # Attempt cleanup
            return False

    def stop(self, notes_content: str = "") -> None:
        """
        Stop the data logging process, turn off instrument output (if not already off),
        close connections, and trigger data export. Pass notes content to be included in export.

        Args:
            notes_content: The content of the notes text field to be included in the export.
        """
        # Assume STATUS_INFO, STATUS_WARNING, STATUS_SUCCESS, STATUS_ERROR are defined elsewhere
        self.status_queue.put((STATUS_INFO, "Stop requested. Stopping logger..."))
        self.stop_event.set()

        # Wait briefly for the logging thread to finish its last loop iteration
        if self.logging_thread and self.logging_thread.is_alive():
             self.logging_thread.join(timeout=2.0) # Wait up to 2 seconds


        # --- Trigger Data Export ---
        # This happens AFTER the logging thread has stopped and data is collected in _logged_data
        # Assume CONFIG_EXPORT_FORMAT is defined elsewhere
        export_format = self.config.get(CONFIG_EXPORT_FORMAT, DEFAULT_EXPORT_FORMAT)
        base_filename = self._generate_base_filename() # Use a helper to get consistent filename
        config_snapshot = dict(self.config) # Use a snapshot of config at time of stopping

        if len(self._logged_data) > 0: # Only export if there is data
            data_to_export = list(self._logged_data) # Use a copy
            headers_to_export = list(self._data_headers) # Use a copy

            # ALWAYS save to CSV when stopping (this was the original behavior, keeping it)
            csv_filename = f"{base_filename}.csv"
            self.status_queue.put((STATUS_INFO, f"Saving data to CSV: {os.path.basename(csv_filename)} ..."))
            DataLogger.save_data_to_csv_static(
                csv_filename, data_to_export, headers_to_export, config_snapshot, notes_content,
                self.status_queue, self.error_queue # Pass queues for status reporting
            )

            # Save to other formats based on selected export_format
            if pd:
                if export_format in ["xlsx", "all"]:
                    excel_filename = f"{base_filename}.xlsx"
                    self.status_queue.put((STATUS_INFO, f"Saving data to Excel: {os.path.basename(excel_filename)} ..."))
                    DataLogger.save_data_to_excel_static(
                         excel_filename, data_to_export, headers_to_export, config_snapshot, notes_content,
                         self.status_queue, self.error_queue # Pass queues
                    )
                if export_format in ["json", "all"]:
                    json_filename = f"{base_filename}.json"
                    self.status_queue.put((STATUS_INFO, f"Saving data to JSON: {os.path.basename(json_filename)} ..."))
                    DataLogger.save_data_to_json_static(
                         json_filename, data_to_export, headers_to_export, config_snapshot, notes_content,
                         self.status_queue, self.error_queue # Pass queues
                    )
            elif export_format in ["xlsx", "json", "all"]:
                 # If pandas is missing and user selected non-CSV, show warning
                 self.error_queue.put((STATUS_WARNING, f"Pandas not available. Cannot export to {export_format.upper()}. Only CSV saved."))

        else:
             self.status_queue.put((STATUS_WARNING, "No data logged to export."))


        # Turn off output and close connection
        if not self.is_simulating:
            self._close_connection()
        else:
             self.status_queue.put((STATUS_INFO, "Simulation output turned OFF (Virtual)"))


        self.logging_thread = None # Clear thread reference
        self.status_queue.put((STATUS_SUCCESS, "Logger stopped and cleanup complete.")) # Final status update

    def _generate_base_filename(self) -> str:
        """Generates a consistent base filename for exports."""
        # Assume datetime, os, CONFIG_VOLTAGE, CONFIG_CURRENT, CONFIG_SAVE_LOCATION are imported/defined
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Use .get with default and convert to str for filename safety
        safe_voltage = str(self.config.get(CONFIG_VOLTAGE, 0.0)).replace('.', '_') # Replace dot for filename safety
        safe_current = str(self.config.get(CONFIG_CURRENT, 0.0)).replace('.', '_') # Replace dot for filename safety
        # Ensure save location exists - add directory creation logic if needed elsewhere
        save_location = self.config.get(CONFIG_SAVE_LOCATION, os.getcwd())
        os.makedirs(save_location, exist_ok=True) # Ensure directory exists
        base_filename = os.path.join(
            save_location,
            f"power_log_V{safe_voltage}_A{safe_current}_{timestamp_str}"
        )
        return base_filename


    def _log_data(self) -> None:
        """
        Internal method for the data logging loop. Reads/simulates data,
        stores data in memory, puts data onto a queue for plotting, and
        triggers instrument output off if threshold is met.
        """
        # Assume time, np, STATUS_INFO, STATUS_WARNING, STATUS_ERROR, CONFIG_UPDATE_INTERVAL, CONFIG_THRESHOLD, CONFIG_STOP_CONDITION are imported/defined
        try:
            # --- Get Update Interval ---
            # Use the same interval for data acquisition as for plotting
            original_interval = self.config.get(CONFIG_UPDATE_INTERVAL, 200)
            interval_ms = max(original_interval, MIN_UPDATE_INTERVAL_MS) # Enforce minimum
            if interval_ms != original_interval:
                # Cannot directly update GUI status from this thread, use queue
                self.status_queue.put((STATUS_WARNING, f"Data acquisition interval adjusted to minimum {interval_ms}ms."))
            acquisition_delay_s = interval_ms / 1000.0

            start_time = time.time() # Use time.time() for overall elapsed time
            last_status_update_time = start_time
            points_logged = 0
            # Use .get with default for safety
            threshold = self.config.get(CONFIG_THRESHOLD, 0.0)
            stop_condition = self.config.get(CONFIG_STOP_CONDITION, "below") # Get the new setting

            self.status_queue.put((STATUS_INFO, "Logging data..."))

            while not self.stop_event.is_set():
                loop_start_time = time.perf_counter() # Start time of the current loop iteration
                current_real_time = time.time() # Get current real time for overall elapsed

                elapsed_time = current_real_time - start_time # Overall elapsed time for the data point

                voltage_measured = 0.0
                current_measured = 0.0

                # --- Data Acquisition (Real or Simulated) ---
                if not self.is_simulating:
                    if not self.dp: # Safety check if connection lost unexpectedly
                         self.error_queue.put((STATUS_ERROR, "Instrument connection lost during logging."))
                         break
                    try:
                        # Assume pyvisa.errors.VisaIOError is imported
                        voltage_measured_str = self.dp.query(":MEAS:VOLT? CH1")
                        current_measured_str = self.dp.query(":MEAS:CURR? CH1")

                        # Validate and convert measurements
                        voltage_measured = float(voltage_measured_str)
                        current_measured = float(current_measured_str)

                    except ValueError:
                         self.error_queue.put((STATUS_WARNING, f"Could not convert measurement: V='{voltage_measured_str}', A='{current_measured_str}'. Skipping point."))
                         # Still need to sleep to attempt to maintain interval
                         time_taken_in_loop = time.perf_counter() - loop_start_time
                         sleep_duration = acquisition_delay_s - time_taken_in_loop
                         if sleep_duration > 0:
                             time.sleep(sleep_duration)
                         continue # Skip logging this point

                    except pyvisa.errors.VisaIOError as e:
                        error_msg = f"VISA IO Error during logging: {e}. Logging stopped."
                        self.error_queue.put((STATUS_ERROR, error_msg))
                        self.stop_event.set() # Signal loop exit
                        break
                    except Exception as e:
                        self.error_queue.put((STATUS_ERROR, f"Unexpected error during data acquisition: {e}"))
                        traceback.print_exc()
                        self.stop_event.set() # Signal loop exit
                        break
                else:
                    # --- Simulated Data Generation ---
                    time_in_sim = current_real_time - (self._sim_start_time or current_real_time)
                    voltage_measured = self._sim_voltage
                    decay_rate = 0.05 # Amps per second decay
                    noise = np.random.randn() * 0.005 # Small random noise
                    current_measured = max(0, self._sim_current - decay_rate * time_in_sim + noise)


                # --- Common Data Processing and Storage ---
                power = voltage_measured * current_measured
                resistance = voltage_measured / current_measured if abs(current_measured) > 1e-9 else float('inf')

                data_row = [elapsed_time, voltage_measured, current_measured, power, resistance]

                # Store in memory
                self._logged_data.append(data_row)
                points_logged += 1

                # Send to queue for plotting
                if hasattr(self, 'data_manager') and self.data_manager:
                     self.data_manager.put(data_row)


                # Check status update interval (e.g., every 5 seconds)
                if current_real_time - last_status_update_time >= 5.0:
                    self.status_queue.put((STATUS_INFO, f"Logging: {points_logged} points collected. Current: {current_measured:.4f} A"))
                    last_status_update_time = current_real_time

                # Check threshold after initial settling time
                stop_triggered = False
                if elapsed_time >= INITIAL_SETTLING_TIME_S:
                    if stop_condition == "below":
                        if abs(current_measured) < threshold:
                            self.status_queue.put((STATUS_WARNING, f"Threshold reached: Current {current_measured:.4f} A < {threshold:.4f} A. Stopping."))
                            stop_triggered = True
                    elif stop_condition == "above":
                        if abs(current_measured) > threshold:
                            self.status_queue.put((STATUS_WARNING, f"Threshold reached: Current {current_measured:.4f} A > {threshold:.4f} A. Stopping."))
                            stop_triggered = True

                if stop_triggered:
                    # Turn off power supply immediately
                    if not self.is_simulating and self.dp:
                        try:
                            self.dp.write(":OUTP CH1,OFF")
                            self.status_queue.put((STATUS_INFO, "Power output turned OFF by threshold trigger."))
                        except pyvisa.errors.VisaIOError as e:
                            self.error_queue.put((STATUS_ERROR, f"VISA IO Error turning off output on threshold: {e}"))
                        except Exception as e:
                            self.error_queue.put((STATUS_ERROR, f"Error turning off output on threshold: {e}"))
                    elif self.is_simulating:
                         self.status_queue.put((STATUS_INFO, "Simulation output turned OFF by threshold trigger (Virtual)."))

                    if winsound:
                        try:
                            winsound.Beep(1000, 500)
                        except Exception as sound_e:
                             self.error_queue.put((STATUS_WARNING, f"Could not play sound: {sound_e}"))

                    self.stop_event.set() # Signal loop exit
                    self.status_queue.put(("STOP_SIGNAL", "Threshold Auto-Stop Triggered")) # Signal main thread
                    break # Exit loop

                # --- Introduce delay ---
                loop_end_time = time.perf_counter()
                time_taken_in_loop = loop_end_time - loop_start_time
                sleep_duration = acquisition_delay_s - time_taken_in_loop
                if sleep_duration > 0 and not self.stop_event.is_set():
                     time.sleep(sleep_duration)

            # --- Logging loop finished ---
            self.status_queue.put((STATUS_INFO, f"Logging loop finished. Total points: {points_logged}."))

        except Exception as e:
            self.error_queue.put((STATUS_ERROR, f"Critical error in logging thread: {e}"))
            traceback.print_exc()
        finally:
            self.status_queue.put(("LOGGER_FINISHED", "Logger thread has completed execution."))


    # --- Static Save Methods ---
    @staticmethod
    def save_data_to_excel_static(
        excel_filename: str,
        data_to_export: List[List[float]],
        data_headers: List[str],
        config_snapshot: Dict[str, Any],
        notes_content: str,
        status_queue: Optional[queue.Queue] = None,
        error_queue: Optional[queue.Queue] = None
    ) -> None:
        """
        Static method to save data, config snapshot, and notes to an Excel file.

        Args:
            excel_filename: Full path for the Excel file.
            data_to_export: List of lists containing the data rows.
            data_headers: List of strings for the data column headers.
            config_snapshot: Dictionary containing relevant configuration at time of save.
            notes_content: String content of the notes.
            status_queue: Optional queue for success messages.
            error_queue: Optional queue for error/warning messages.
        """
        if not pd or not openpyxl:
            if error_queue:
                 error_queue.put((STATUS_WARNING, "Pandas or openpyxl library not found. Cannot export to Excel."))
            else:
                 print("Warning: Pandas or openpyxl library not found. Cannot export to Excel.")
            return

        if not data_to_export:
            if status_queue:
                 status_queue.put((STATUS_WARNING, f"No data provided to save to Excel."))
            else:
                 print("Warning: No data provided to save to Excel.")
            return

        try:
            df_data = pd.DataFrame(data_to_export, columns=data_headers)
            config_data = {
                "Selected Voltage (V)": config_snapshot.get(CONFIG_VOLTAGE, "N/A"),
                "Selected Current (A)": config_snapshot.get(CONFIG_CURRENT, "N/A"),
                "Operation Mode": config_snapshot.get(CONFIG_OPERATION_MODE, "N/A"), # New
                "Selected Threshold (A)": config_snapshot.get(CONFIG_THRESHOLD, "N/A"),
                "Stop Condition": config_snapshot.get(CONFIG_STOP_CONDITION, "N/A"),
                "Anode": config_snapshot.get(CONFIG_ANODE, "N/A"),
                "Cathode": config_snapshot.get(CONFIG_CATHODE, "N/A"),
                "Electrolyte": config_snapshot.get(CONFIG_ELECTROLYTE, "N/A"),
                "Electrolyte Molarity (M)": config_snapshot.get(CONFIG_ELECTROLYTE_MOLARITY, "N/A"),
                # Add other relevant config items if needed
            }
            config_df = pd.DataFrame.from_dict(config_data, orient='index', columns=['Value'])
            notes_lines = notes_content.splitlines()
            notes_df = pd.DataFrame(notes_lines, columns=["Notes"])

            try:
                 with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                     df_data.to_excel(writer, sheet_name='Data', index=False)
                     config_df.to_excel(writer, sheet_name='Settings', index=True, header=True)
                     notes_df.to_excel(writer, sheet_name='Notes', index=False, header=True)

                 msg = f"Data saved to Excel: {os.path.basename(excel_filename)} (incl settings and notes)"
                 if status_queue:
                      status_queue.put((STATUS_SUCCESS, msg))
                 else:
                      print(f"Success: {msg}")

            except IOError as e:
                 err_msg = f"File I/O error saving Excel: {e}. Check permissions or if file is open."
                 if error_queue: error_queue.put((STATUS_ERROR, err_msg))
                 else: print(f"Error: {err_msg}")
                 traceback.print_exc()
            except Exception as e:
                 err_msg = f"Error saving data to Excel: {e}"
                 if error_queue: error_queue.put((STATUS_ERROR, err_msg))
                 else: print(f"Error: {err_msg}")
                 traceback.print_exc()

        except Exception as e:
            err_msg = f"Error preparing data for Excel save: {e}"
            if error_queue: error_queue.put((STATUS_ERROR, err_msg))
            else: print(f"Error: {err_msg}")
            traceback.print_exc()

    @staticmethod
    def save_data_to_json_static(
        json_filename: str,
        data_to_export: List[List[float]],
        data_headers: List[str],
        config_snapshot: Dict[str, Any],
        notes_content: str,
        status_queue: Optional[queue.Queue] = None,
        error_queue: Optional[queue.Queue] = None
    ) -> None:
        """
        Static method to save data, config snapshot, and notes to a JSON file.

        Args:
            json_filename: Full path for the JSON file.
            data_to_export: List of lists containing the data rows.
            data_headers: List of strings for the data column headers.
            config_snapshot: Dictionary containing relevant configuration at time of save.
            notes_content: String content of the notes.
            status_queue: Optional queue for success messages.
            error_queue: Optional queue for error/warning messages.
        """
        if not pd:
            if error_queue:
                 error_queue.put((STATUS_WARNING, "Pandas library not found. Cannot export to JSON."))
            else:
                 print("Warning: Pandas library not found. Cannot export to JSON.")
            return

        if not data_to_export:
            if status_queue:
                 status_queue.put((STATUS_WARNING, f"No data provided to save to JSON."))
            else:
                 print("Warning: No data provided to save to JSON.")
            return

        try:
            df_data = pd.DataFrame(data_to_export, columns=data_headers)
            data_records = df_data.to_dict(orient="records")
            config_data = {
                "Selected Voltage (V)": config_snapshot.get(CONFIG_VOLTAGE, "N/A"),
                "Selected Current (A)": config_snapshot.get(CONFIG_CURRENT, "N/A"),
                "Operation Mode": config_snapshot.get(CONFIG_OPERATION_MODE, "N/A"), # New
                "Selected Threshold (A)": config_snapshot.get(CONFIG_THRESHOLD, "N/A"),
                "Stop Condition": config_snapshot.get(CONFIG_STOP_CONDITION, "N/A"),
                "Anode": config_snapshot.get(CONFIG_ANODE, "N/A"),
                "Cathode": config_snapshot.get(CONFIG_CATHODE, "N/A"),
                "Electrolyte": config_snapshot.get(CONFIG_ELECTROLYTE, "N/A"),
                "Electrolyte Molarity": config_snapshot.get(CONFIG_ELECTROLYTE_MOLARITY, "N/A"),
                # Add other relevant config items if needed
            }
            json_output = {
                "settings": config_data,
                "notes": notes_content,
                "data": data_records
            }

            try:
                 with open(json_filename, 'w', encoding='utf-8') as f:
                     json.dump(json_output, f, indent=4)

                 msg = f"Data saved to JSON: {os.path.basename(json_filename)} (incl settings and notes)"
                 if status_queue:
                      status_queue.put((STATUS_SUCCESS, msg))
                 else:
                      print(f"Success: {msg}")

            except IOError as e:
                 err_msg = f"File I/O error saving JSON: {e}. Check permissions or if file is open."
                 if error_queue: error_queue.put((STATUS_ERROR, err_msg))
                 else: print(f"Error: {err_msg}")
                 traceback.print_exc()
            except Exception as e:
                 err_msg = f"Error saving data to JSON: {e}"
                 if error_queue: error_queue.put((STATUS_ERROR, err_msg))
                 else: print(f"Error: {err_msg}")
                 traceback.print_exc()

        except Exception as e:
            err_msg = f"Error preparing data for JSON save: {e}"
            if error_queue: error_queue.put((STATUS_ERROR, err_msg))
            else: print(f"Error: {err_msg}")
            traceback.print_exc()

    @staticmethod
    def save_data_to_csv_static(
        csv_filename: str,
        data_to_export: List[List[float]],
        data_headers: List[str],
        config_snapshot: Dict[str, Any],
        notes_content: str,
        status_queue: Optional[queue.Queue] = None,
        error_queue: Optional[queue.Queue] = None
    ) -> None:
        """
        Static method to save data, config snapshot (as comments), and notes (as comments) to a CSV file.

        Args:
            csv_filename: Full path for the CSV file.
            data_to_export: List of lists containing the data rows.
            data_headers: List of strings for the data column headers.
            config_snapshot: Dictionary containing relevant configuration at time of save.
            notes_content: String content of the notes.
            status_queue: Optional queue for success messages.
            error_queue: Optional queue for error/warning messages.
        """
        if not data_to_export:
            if status_queue:
                 status_queue.put((STATUS_WARNING, f"No data provided to save to CSV."))
            else:
                 print("Warning: No data provided to save to CSV.")
            return

        try:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
                 csv_writer = csv.writer(csv_file)

                 csv_writer.writerow(['# --- Configuration ---'])
                 csv_writer.writerow([f'# RESOURCE_NAME: {config_snapshot.get(CONFIG_RESOURCE_NAME, "N/A")}'])
                 csv_writer.writerow([f'# OPERATION_MODE: {config_snapshot.get(CONFIG_OPERATION_MODE, "N/A")}']) # New
                 csv_writer.writerow([f'# VOLTAGE_SETTING: {config_snapshot.get(CONFIG_VOLTAGE, "N/A")} V'])
                 csv_writer.writerow([f'# CURRENT_SETTING: {config_snapshot.get(CONFIG_CURRENT, "N/A")} A'])
                 csv_writer.writerow([f'# STOP_THRESHOLD: {config_snapshot.get(CONFIG_THRESHOLD, "N/A")} A'])
                 csv_writer.writerow([f'# STOP_CONDITION: {config_snapshot.get(CONFIG_STOP_CONDITION, "N/A")}'])
                 csv_writer.writerow([f'# ANODE: {config_snapshot.get(CONFIG_ANODE, "N/A")}'])
                 csv_writer.writerow([f'# CATHODE: {config_snapshot.get(CONFIG_CATHODE, "N/A")}'])
                 csv_writer.writerow([f'# ELECTROLYTE: {config_snapshot.get(CONFIG_ELECTROLYTE, "N/A")}'])
                 csv_writer.writerow([f'# Electrolyte Molarity: {config_snapshot.get(CONFIG_ELECTROLYTE_MOLARITY, "N/A")} M'])
                 # Add other relevant config items if needed

                 csv_writer.writerow(['# --- Notes ---'])
                 if notes_content:
                      for line in notes_content.splitlines():
                           csv_writer.writerow([f'# {line}'])
                 else:
                      csv_writer.writerow(['# No notes provided.'])

                 csv_writer.writerow(['# --- Data ---'])
                 csv_writer.writerow(data_headers)
                 csv_writer.writerows(data_to_export)

            msg = f"Data saved to CSV: {os.path.basename(csv_filename)} (incl settings and notes as comments)"
            if status_queue:
                 status_queue.put((STATUS_SUCCESS, msg))
            else:
                 print(f"Success: {msg}")

        except IOError as e:
            err_msg = f"File I/O error saving CSV: {e}. Check permissions or if file is open."
            if error_queue: error_queue.put((STATUS_ERROR, err_msg))
            else: print(f"Error: {err_msg}")
            traceback.print_exc()
        except Exception as e:
            err_msg = f"Error during CSV save process: {e}"
            if error_queue: error_queue.put((STATUS_ERROR, err_msg))
            else: print(f"Error: {err_msg}")
            traceback.print_exc()

    # --- Instance Save Methods (Call Static Methods) ---
    def _save_data_to_excel(self, excel_filename: str, notes_content: str) -> None:
        """ Saves data using the static method. """
        DataLogger.save_data_to_excel_static(
            excel_filename,
            list(self._logged_data), # Pass copy
            list(self._data_headers),# Pass copy
            dict(self.config),       # Pass copy
            notes_content,
            self.status_queue,
            self.error_queue
        )

    def _save_data_to_json(self, json_filename: str, notes_content: str) -> None:
        """ Saves data using the static method. """
        DataLogger.save_data_to_json_static(
            json_filename,
            list(self._logged_data), # Pass copy
            list(self._data_headers),# Pass copy
            dict(self.config),       # Pass copy
            notes_content,
            self.status_queue,
            self.error_queue
        )

    def _save_data_to_csv(self, csv_filename: str, notes_content: str) -> None:
        """ Saves data using the static method. """
        DataLogger.save_data_to_csv_static(
            csv_filename,
            list(self._logged_data), # Pass copy
            list(self._data_headers),# Pass copy
            dict(self.config),       # Pass copy
            notes_content,
            self.status_queue,
            self.error_queue
        )


# --- Tkinter Application ---
class PowerLoggerApp:
    """
    Tkinter application for logging power measurements.
    """
    def __init__(self, root: ThemedTk):
        """
        Initializes the main application window.

        Args:
            root: The ThemedTk root window.
        """
        self.root = root

        # Set window icon
        try:
            icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app_icon.ico")
            if Image and ImageTk and os.path.exists(icon_path):
                pil_image = Image.open(icon_path)
                tk_image = ImageTk.PhotoImage(pil_image)
                self.root.iconphoto(True, tk_image)
            else:
                 if not (Image and ImageTk):
                      print("Warning: Pillow not imported. Cannot use iconphoto for advanced image types.")
                 elif not os.path.exists(icon_path):
                      print(f"Info: Icon file '{os.path.basename(icon_path)}' not found.")

        except tk.TclError as e:
             print(f"Warning: Could not set window icon (TclError): {e}.")
        except Exception as e:
             print(f"Warning: Could not set window icon: {e}.")


        # Managers and Queues
        self.config_manager = ConfigManager()
        self.data_manager = DataManager(max_plot_points=self.config_manager.config.get(CONFIG_MAX_PLOT_POINTS, 1000))
        self.status_queue: queue.Queue[Tuple[str, str]] = queue.Queue()
        self.error_queue: queue.Queue[Tuple[str, str]] = queue.Queue()

        # Logger and State
        self.logger: Optional[DataLogger] = None
        self.is_logging: bool = False
        self.executor = ThreadPoolExecutor(max_workers=2) # For connect/scan tasks

        # Variable to store the after ID for check_queues
        self._after_check_queues_id: Optional[str] = None
        self.ani: Optional[animation.FuncAnimation] = None


        # Initialize widget attributes to None
        self.notebook: Optional[ttk.Notebook] = None
        self.main_tab: Optional[ttk.Frame] = None
        self.plot_tab: Optional[ttk.Frame] = None
        self.settings_tab: Optional[ttk.Frame] = None
        self.log_tab: Optional[ttk.Frame] = None
        self.notes_tab: Optional[ttk.Frame] = None
        self.resource_combobox: Optional[ttk.Combobox] = None
        self.scan_button: Optional[ttk.Button] = None
        self.voltage_entry: Optional[ttk.Entry] = None
        self.current_entry: Optional[ttk.Entry] = None
        self.threshold_entry: Optional[ttk.Entry] = None
        self.radio_stop_below: Optional[ttk.Radiobutton] = None
        self.radio_stop_above: Optional[ttk.Radiobutton] = None
        self.save_location_entry: Optional[ttk.Entry] = None
        self.browse_button: Optional[ttk.Button] = None
        self.export_format_radios: List[ttk.Radiobutton] = []
        self.operation_mode_radios: List[ttk.Radiobutton] = [] # New
        self.status_indicator: Optional[ttk.Label] = None
        self.start_button: Optional[ttk.Button] = None
        self.stop_button: Optional[ttk.Button] = None
        self.update_interval_entry: Optional[ttk.Entry] = None
        self.max_plot_points_entry: Optional[ttk.Entry] = None
        self.gui_theme_combobox: Optional[ttk.Combobox] = None
        self.plot_style_combobox: Optional[ttk.Combobox] = None
        self.simulation_mode_check: Optional[ttk.Checkbutton] = None
        self.preset_name_entry: Optional[ttk.Entry] = None
        self.save_preset_button: Optional[ttk.Button] = None
        self.delete_preset_button: Optional[ttk.Button] = None
        self.load_preset_button: Optional[ttk.Button] = None
        self.preset_listbox: Optional[tk.Listbox] = None
        self.log_text: Optional[tk.Text] = None
        self.notes_text: Optional[tk.Text] = None
        self.fig: Optional[Figure] = None
        self.ax_v: Optional[Axes] = None
        self.ax_i: Optional[Axes] = None
        self.ax_p: Optional[Axes] = None
        self.ax_r: Optional[Axes] = None
        self.line_v: Optional[Line2D] = None
        self.line_i: Optional[Line2D] = None
        self.line_p: Optional[Line2D] = None
        self.line_r: Optional[Line2D] = None
        self.canvas: Optional[FigureCanvasTkAgg] = None
        self.canvas_widget: Optional[tk.Widget] = None
        self.anode_entry: Optional[ttk.Entry] = None
        self.cathode_entry: Optional[ttk.Entry] = None
        self.electrolyte_entry: Optional[ttk.Entry] = None
        self.electrolyte_molarity_entry: Optional[ttk.Entry] = None
        self.enable_theme_fade_check: Optional[ttk.Checkbutton] = None


        # UI Setup
        try:
             self._setup_variables()
        except Exception as e:
             self._handle_initialization_error(f"Error during UI variable setup: {e}")
             return

        try:
             self._apply_theme(self.config_manager.config.get(CONFIG_GUI_THEME, DEFAULT_GUI_THEME))
        except Exception as e:
             self.add_status_message(f"Error applying initial theme: {e}", STATUS_ERROR)

        # Window Setup
        self.root.title("Power Supply Stopper")
        self.root.geometry("950x750")
        self.root.minsize(800, 600)
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        try:
             self._setup_ui()
        except Exception as e:
             self._handle_initialization_error(f"Error setting up main UI: {e}")
             return

        try:
             self._load_initial_values()
        except Exception as e:
             self.add_status_message(f"Warning: Error loading initial settings: {e}", STATUS_WARNING)


        # Plotting Setup
        try:
            self._create_plot_figure()
            self._create_plot_canvas()
            self._create_plot_lines()
            self._create_plot_toolbar()
        except Exception as e:
            self._handle_initialization_error(f"Error setting up plots: {e}")


        # Start queue checking loop
        if self.root.winfo_exists():
            self.check_queues()
        else:
            self._handle_initialization_error("Root window not available to start queue checker.")
            return


        # Initial VISA scan
        try:
            if pyvisa:
                self.scan_visa_resources()
            else:
                self.add_status_message("PyVISA not found. Instrument connection disabled.", STATUS_WARNING)
                if hasattr(self, 'resource_combobox') and self.resource_combobox and self.resource_combobox.winfo_exists():
                     self.resource_combobox.config(values=["PyVISA Missing"])
        except Exception as e:
             self.add_status_message(f"Warning: Error during initial VISA scan: {e}", STATUS_WARNING)

        # Ensure UI state is correct
        try:
             self.update_ui_state(self.is_logging)
        except Exception as e:
             self.add_status_message(f"Warning: Error updating initial UI state: {e}", STATUS_WARNING)


    def _handle_initialization_error(self, message: str) -> None:
        """Handles errors that occur during application initialization."""
        print(f"Initialization Error: {message}")
        try:
            is_tkinter_ready = False
            if hasattr(tk, '_default_root') and tk._default_root is not None:
                 if tk._default_root.winfo_exists():
                      is_tkinter_ready = True

            if is_tkinter_ready:
                messagebox.showerror("Initialization Error", "The application failed to initialize:\n\n" + message)
                if self.root and self.root.winfo_exists():
                     self.root.destroy()
            else:
                print("Could not show initialization error in messagebox (Tkinter not ready).")
        except Exception as e:
            print(f"Error showing initialization error messagebox: {e}")
        sys.exit(1)


    def _setup_variables(self) -> None:
        """Initialize Tkinter control variables."""
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
        self.enable_theme_fade_var = BooleanVar(value=DEFAULT_ENABLE_THEME_FADE)
        self.anode_var = StringVar()
        self.cathode_var = StringVar()
        self.electrolyte_var = StringVar()
        self.electrolyte_molarity_var = StringVar()
        self.preset_name_var = StringVar()
        self.operation_mode_var = StringVar() # New


    def _load_initial_values(self) -> None:
        """Load values from config into Tkinter variables and load notes."""
        if hasattr(self, 'resource_var'):
             self.resource_var.set(self.config_manager.config.get(CONFIG_RESOURCE_NAME, ""))
        if hasattr(self, 'voltage_var'):
             self.voltage_var.set(str(self.config_manager.config.get(CONFIG_VOLTAGE, 4.0)))
        if hasattr(self, 'current_var'):
             self.current_var.set(str(self.config_manager.config.get(CONFIG_CURRENT, 0.5)))
        if hasattr(self, 'threshold_var'):
             self.threshold_var.set(str(self.config_manager.config.get(CONFIG_THRESHOLD, 0.062)))
        if hasattr(self, 'stop_condition_var'):
             self.stop_condition_var.set(self.config_manager.config.get(CONFIG_STOP_CONDITION, "below"))
        if hasattr(self, 'save_location_var'):
             self.save_location_var.set(self.config_manager.config.get(CONFIG_SAVE_LOCATION, self.config_manager.base_path))
        if hasattr(self, 'export_format_var'):
             self.export_format_var.set(self.config_manager.config.get(CONFIG_EXPORT_FORMAT, DEFAULT_EXPORT_FORMAT))
        if hasattr(self, 'update_interval_var'):
             self.update_interval_var.set(str(self.config_manager.config.get(CONFIG_UPDATE_INTERVAL, 200)))
        if hasattr(self, 'max_plot_points_var'):
             self.max_plot_points_var.set(str(self.config_manager.config.get(CONFIG_MAX_PLOT_POINTS, 1000)))
        if hasattr(self, 'gui_theme_var'):
             self.gui_theme_var.set(self.config_manager.config.get(CONFIG_GUI_THEME, DEFAULT_GUI_THEME))
        if hasattr(self, 'plot_style_var'):
             self.plot_style_var.set(self.config_manager.config.get(CONFIG_PLOT_STYLE, DEFAULT_PLOT_STYLE))
        if hasattr(self, 'simulation_mode_var'):
             self.simulation_mode_var.set(self.config_manager.config.get(CONFIG_SIMULATION_MODE, False))
        if hasattr(self, 'preset_name_var'):
             self.preset_name_var.set("")
        if hasattr(self, 'enable_theme_fade_var'):
            self.enable_theme_fade_var.set(self.config_manager.config.get(CONFIG_ENABLE_THEME_FADE, DEFAULT_ENABLE_THEME_FADE))
        if hasattr(self, 'anode_var'):
            self.anode_var.set(self.config_manager.config.get(CONFIG_ANODE, "N/A"))
        if hasattr(self, 'cathode_var'):
            self.cathode_var.set(self.config_manager.config.get(CONFIG_CATHODE, "N/A"))
        if hasattr(self, 'electrolyte_var'):
            self.electrolyte_var.set(self.config_manager.config.get(CONFIG_ELECTROLYTE, "N/A"))
        if hasattr(self, 'electrolyte_molarity_var'):
            self.electrolyte_molarity_var.set(self.config_manager.config.get(CONFIG_ELECTROLYTE_MOLARITY, "N/A"))
        if hasattr(self, 'operation_mode_var'): # New
            self.operation_mode_var.set(self.config_manager.config.get(CONFIG_OPERATION_MODE, MODE_CONSTANT_VOLTAGE))

        self.load_notes()


    def _setup_ui(self) -> None:
        """Set up the main UI components and tabs."""
        if hasattr(self, 'notebook') and self.notebook and self.notebook.winfo_exists():
            self.notebook.destroy()
            self.notebook = None
        if not self.root.winfo_exists():
             return

        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=UI_PADDING_X, pady=UI_PADDING_Y)

        self.main_tab = ttk.Frame(self.notebook)
        self.plot_tab = ttk.Frame(self.notebook)
        self.settings_tab = ttk.Frame(self.notebook)
        self.log_tab = ttk.Frame(self.notebook)
        self.notes_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.main_tab, text=" Control ")
        self.notebook.add(self.plot_tab, text=" Plots ")
        self.notebook.add(self.settings_tab, text=" Settings ")
        self.notebook.add(self.log_tab, text=" Log ")
        self.notebook.add(self.notes_tab, text=" Notes ")

        self._setup_control_tab()
        self._setup_plot_tab()
        self._setup_settings_tab()
        self._setup_log_tab()
        self._setup_notes_tab()

    def _setup_settings_tab(self) -> None:
        """Set up the settings tab UI."""
        if not hasattr(self, 'settings_tab') or not self.settings_tab or not self.settings_tab.winfo_exists():
             return

        pad_args = {'padx': WIDGET_PADDING_X, 'pady': WIDGET_PADDING_Y}
        grid_args = {**pad_args, 'sticky': GRID_STICKY_W}
        grid_args_entry = {**pad_args, 'sticky': GRID_STICKY_EW}

        # --- General Settings ---
        general_settings_frame = ttk.LabelFrame(self.settings_tab, text="General Settings")
        general_settings_frame.pack(fill=tk.X, padx=FRAME_PADDING_X, pady=(FRAME_PADDING_Y_TOP, FRAME_PADDING_Y_BOTTOM))

        row = 0
        # Plot Update Interval
        ttk.Label(general_settings_frame, text="Plot Update Interval (ms):").grid(row=row, column=0, **grid_args)
        update_interval_frame = ttk.Frame(general_settings_frame, **FRAME_STYLE)
        update_interval_frame.grid(row=row, column=1, **grid_args_entry)
        self.update_interval_entry = ttk.Entry(update_interval_frame, textvariable=self.update_interval_var)
        self.update_interval_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.update_interval_entry, f"Frequency of data acquisition and plot updates in milliseconds (Min {MIN_UPDATE_INTERVAL_MS}ms).")
        row += 1

        # Max Plot Data Points
        ttk.Label(general_settings_frame, text="Max Plot Data Points:").grid(row=row, column=0, **grid_args)
        max_plot_points_frame = ttk.Frame(general_settings_frame, **FRAME_STYLE)
        max_plot_points_frame.grid(row=row, column=1, **grid_args_entry)
        self.max_plot_points_entry = ttk.Entry(max_plot_points_frame, textvariable=self.max_plot_points_var)
        self.max_plot_points_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.max_plot_points_entry, f"Maximum number of data points displayed on the plot (sliding window, Min {MIN_PLOT_POINTS}).")
        row += 1

        # Simulation Mode
        ttk.Label(general_settings_frame, text="Simulation Mode:").grid(row=row, column=0, **grid_args)
        if hasattr(self, 'simulation_mode_var'):
            self.simulation_mode_check = ttk.Checkbutton(general_settings_frame, variable=self.simulation_mode_var)
            self.simulation_mode_check.grid(row=row, column=1, **grid_args)
            Tooltip(self.simulation_mode_check, "Check to run without a physical instrument.")
        row += 1

        # Stop Condition Radio Buttons
        ttk.Label(general_settings_frame, text="Stop When Current Is:").grid(row=row, column=0, **grid_args)
        stop_condition_frame = ttk.Frame(general_settings_frame)
        stop_condition_frame.grid(row=row, column=1, **grid_args)
        if hasattr(self, 'stop_condition_var'):
             self.radio_stop_below = ttk.Radiobutton(stop_condition_frame, text="Below Threshold", variable=self.stop_condition_var, value="below")
             self.radio_stop_below.pack(side=tk.LEFT, padx=5)
             self.radio_stop_above = ttk.Radiobutton(stop_condition_frame, text="Above Threshold", variable=self.stop_condition_var, value="above")
             self.radio_stop_above.pack(side=tk.LEFT, padx=5)
        row += 1

        general_settings_frame.columnconfigure(1, weight=1)

        # --- Appearance Settings ---
        appearance_frame = ttk.LabelFrame(self.settings_tab, text="Appearance Settings")
        appearance_frame.pack(fill=tk.X, padx=FRAME_PADDING_X, pady=(FRAME_PADDING_Y_TOP, FRAME_PADDING_Y_BOTTOM))

        row = 0
        # GUI Theme
        ttk.Label(appearance_frame, text="GUI Theme:").grid(row=row, column=0, **grid_args)
        theme_frame = ttk.Frame(appearance_frame, **FRAME_STYLE)
        theme_frame.grid(row=row, column=1, **grid_args_entry)
        if isinstance(self.root, ThemedTk):
            available_themes = sorted(self.root.get_themes())
        else:
            available_themes = ["default (ttkthemes not installed)"]

        if hasattr(self, 'gui_theme_var'):
            self.gui_theme_combobox = ttk.Combobox(theme_frame, textvariable=self.gui_theme_var, state="readonly", values=available_themes)
            self.gui_theme_combobox.pack(fill=tk.X, expand=True)
            self.gui_theme_combobox.bind("<<ComboboxSelected>>", self.on_gui_theme_selected)
            Tooltip(self.gui_theme_combobox, "Select a visual theme for the application.")
        row += 1

        # Enable Theme Fade
        ttk.Label(appearance_frame, text="Enable Theme Fade:").grid(row=row, column=0, **grid_args)
        if hasattr(self, 'enable_theme_fade_var'):
            self.enable_theme_fade_check = ttk.Checkbutton(appearance_frame, variable=self.enable_theme_fade_var)
            self.enable_theme_fade_check.grid(row=row, column=1, **grid_args)
            Tooltip(self.enable_theme_fade_check, "Enable a subtle fade effect when changing themes.")
        row += 1


        # Plot Style
        ttk.Label(appearance_frame, text="Plot Style:").grid(row=row, column=0, **grid_args)
        plot_style_frame = ttk.Frame(appearance_frame, **FRAME_STYLE)
        plot_style_frame.grid(row=row, column=1, **grid_args_entry)
        available_plot_styles = sorted(plt.style.available)

        if hasattr(self, 'plot_style_var'):
            self.plot_style_combobox = ttk.Combobox(plot_style_frame, textvariable=self.plot_style_var, state="readonly", values=available_plot_styles)
            self.plot_style_combobox.pack(fill=tk.X, expand=True)
            self.plot_style_combobox.bind("<<ComboboxSelected>>", self.on_plot_style_selected)
            Tooltip(self.plot_style_combobox, "Select a visual style for the plots.")
        row += 1

        appearance_frame.columnconfigure(1, weight=1)

        # --- Presets ---
        presets_frame = ttk.LabelFrame(self.settings_tab, text="Presets")
        presets_frame.pack(fill=tk.BOTH, expand=True, padx=FRAME_PADDING_X, pady=(FRAME_PADDING_Y_TOP, UI_PADDING_Y))

        row = 0
        # Preset Name
        ttk.Label(presets_frame, text="Preset Name:").grid(row=row, column=0, **grid_args)
        preset_name_frame = ttk.Frame(presets_frame, **FRAME_STYLE)
        preset_name_frame.grid(row=row, column=1, **grid_args_entry)
        if hasattr(self, 'preset_name_var'):
            self.preset_name_entry = ttk.Entry(preset_name_frame, textvariable=self.preset_name_var)
            self.preset_name_entry.pack(fill=tk.X, expand=True)
            Tooltip(self.preset_name_entry, "Enter a name to save or load a preset.")
        row += 1

        # Preset Buttons
        button_frame = ttk.Frame(presets_frame)
        button_frame.grid(row=row, column=0, columnspan=2, pady=WIDGET_PADDING_Y, sticky=GRID_STICKY_EW)

        self.save_preset_button = ttk.Button(button_frame, text="Save Preset", command=self.save_preset)
        self.save_preset_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)

        self.load_preset_button = ttk.Button(button_frame, text="Load Preset", command=self.load_preset)
        self.load_preset_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)

        self.delete_preset_button = ttk.Button(button_frame, text="Delete Preset", command=self.delete_preset)
        self.delete_preset_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)
        row += 1

        # Preset Listbox
        ttk.Label(presets_frame, text="Available Presets:").grid(row=row, column=0, **grid_args)
        listbox_frame = ttk.Frame(presets_frame, **FRAME_STYLE)
        listbox_frame.grid(row=row, column=1, sticky=tk.NSEW, **pad_args)

        self.preset_listbox = tk.Listbox(listbox_frame, height=5)
        self.preset_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        listbox_scrollbar = ttk.Scrollbar(listbox_frame, command=self.preset_listbox.yview)
        listbox_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.preset_listbox.config(yscrollcommand=listbox_scrollbar.set)
        self.preset_listbox.bind("<Double-Button-1>", lambda event: self.load_preset())

        row += 1
        self.load_presets_listbox()

        presets_frame.columnconfigure(1, weight=1)
        presets_frame.rowconfigure(row-1, weight=1)

    def _setup_log_tab(self) -> None:
        """Set up the log tab UI."""
        if not hasattr(self, 'log_tab') or not self.log_tab or not self.log_tab.winfo_exists():
             return

        log_frame = ttk.Frame(self.log_tab)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=UI_PADDING_X, pady=(UI_PADDING_Y,FRAME_PADDING_Y_BOTTOM))

        self.log_text = tk.Text(log_frame, state=tk.DISABLED, wrap=tk.WORD, height=10, width=80, font=('TkFixedFont', 9))
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        log_scrollbar_y = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=log_scrollbar_y.set)

        self.log_text.tag_config(STATUS_INFO, foreground="blue")
        self.log_text.tag_config(STATUS_SUCCESS, foreground="green")
        self.log_text.tag_config(STATUS_WARNING, foreground="dark orange")
        self.log_text.tag_config(STATUS_ERROR, foreground="red")

        button_frame = ttk.Frame(self.log_tab)
        button_frame.pack(fill=tk.X, pady=FRAME_PADDING_Y_BOTTOM, padx=UI_PADDING_X)

        clear_button = ttk.Button(button_frame, text="Clear Log", command=self.clear_log)
        clear_button.pack(side=tk.LEFT)


    def _setup_notes_tab(self) -> None:
        """Set up the Notes tab UI for adding free-form notes."""
        if not hasattr(self, 'notes_tab') or not self.notes_tab or not self.notes_tab.winfo_exists():
             return

        notes_frame = ttk.Frame(self.notes_tab)
        notes_frame.pack(fill=tk.BOTH, expand=True, padx=UI_PADDING_X, pady=(UI_PADDING_Y, FRAME_PADDING_Y_BOTTOM))

        self.notes_text = tk.Text(notes_frame, wrap=tk.WORD, height=10, width=60, font=('TkDefaultFont', 10))
        self.notes_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        notes_scrollbar_y = ttk.Scrollbar(notes_frame, command=self.notes_text.yview)
        notes_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.notes_text.config(yscrollcommand=notes_scrollbar_y.set)

        button_frame = ttk.Frame(self.notes_tab)
        button_frame.pack(fill=tk.X, pady=FRAME_PADDING_Y_BOTTOM, padx=UI_PADDING_X)

        save_button = ttk.Button(button_frame, text="Save Notes", command=self.save_notes)
        save_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)

        load_button = ttk.Button(button_frame, text="Load Notes", command=self.load_notes)
        load_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)


    def save_notes(self) -> None:
        """Gets notes content from the text widget and saves it via config manager."""
        if not hasattr(self, 'notes_text') or not self.notes_text or not self.notes_text.winfo_exists():
             self.add_status_message("Cannot save notes: Notes widget not available.", STATUS_ERROR)
             return

        if hasattr(self, 'config_manager') and self.config_manager:
            try:
                notes_content = self.notes_text.get("1.0", tk.END).strip()
                self.config_manager.save_notes(notes_content)
                self.config_manager.save_config()
                self.add_status_message("Notes saved.", STATUS_SUCCESS)
            except Exception as e:
                 self.add_status_message(f"Error saving notes: {e}", STATUS_ERROR)
                 traceback.print_exc()
        else:
             self.add_status_message("Config manager not available to save notes.", STATUS_ERROR)


    def load_notes(self) -> None:
        """Loads notes content from the config manager and sets it in the text widget."""
        if not hasattr(self, 'notes_text') or not self.notes_text or not self.notes_text.winfo_exists():
             return

        if hasattr(self, 'config_manager') and self.config_manager:
            try:
                notes_content = self.config_manager.get_notes()
                self.notes_text.config(state=tk.NORMAL)
                self.notes_text.delete("1.0", tk.END)
                self.notes_text.insert("1.0", notes_content)
            except Exception as e:
                 self.add_status_message(f"Error loading notes: {e}", STATUS_ERROR)
                 traceback.print_exc()
        else:
             self.add_status_message("Config manager not available to load notes.", STATUS_ERROR)


    def _setup_control_tab(self) -> None:
        """Set up the main control tab UI."""
        if not hasattr(self, 'main_tab') or not self.main_tab or not self.main_tab.winfo_exists():
             return

        # --- Frames ---
        control_frame = ttk.LabelFrame(self.main_tab, text="Power Supply Control")
        control_frame.pack(fill=tk.X, padx=UI_PADDING_X, pady=(FRAME_PADDING_Y_TOP,FRAME_PADDING_Y_BOTTOM))

        cell_info_frame = ttk.LabelFrame(self.main_tab, text="Electrochemical Cell Information")
        cell_info_frame.pack(fill=tk.X, padx=UI_PADDING_X, pady=FRAME_PADDING_Y_BOTTOM)

        status_frame = ttk.LabelFrame(self.main_tab, text="Status")
        status_frame.pack(fill=tk.X, padx=UI_PADDING_X, pady=FRAME_PADDING_Y_BOTTOM)

        button_frame = ttk.Frame(self.main_tab)
        button_frame.pack(fill=tk.X, padx=UI_PADDING_X, pady=(FRAME_PADDING_Y_BOTTOM, UI_PADDING_Y))

        pad_args = {'padx': WIDGET_PADDING_X, 'pady': WIDGET_PADDING_Y}
        grid_args = {**pad_args, 'sticky': GRID_STICKY_W}
        grid_args_entry = {**pad_args, 'sticky': GRID_STICKY_EW}

        # --- Controls ---
        row = 0
        # Resource
        ttk.Label(control_frame, text="VISA Resource:").grid(row=row, column=0, **grid_args)
        resource_frame = ttk.Frame(control_frame, **FRAME_STYLE)
        resource_frame.grid(row=row, column=1, columnspan=2, **grid_args_entry)
        self.resource_combobox = ttk.Combobox(resource_frame, textvariable=self.resource_var, state="readonly")
        self.resource_combobox.pack(fill=tk.X, expand=True)
        self.scan_button = ttk.Button(control_frame, text="Scan", command=self.scan_visa_resources, width=6)
        self.scan_button.grid(row=row, column=3, **pad_args)
        row += 1

        # Operation Mode - New
        ttk.Label(control_frame, text="Operation Mode:").grid(row=row, column=0, **grid_args)
        op_mode_frame = ttk.Frame(control_frame)
        op_mode_frame.grid(row=row, column=1, columnspan=3, sticky=GRID_STICKY_W, padx=WIDGET_PADDING_X, pady=WIDGET_PADDING_Y) # Use sticky W
        self.operation_mode_radios = [] # Initialize the list
        modes = [MODE_CONSTANT_VOLTAGE, MODE_CONSTANT_CURRENT]
        for mode_text in modes:
            radio = ttk.Radiobutton(op_mode_frame, text=mode_text, variable=self.operation_mode_var, value=mode_text)
            radio.pack(side=tk.LEFT, padx=(0, 10))
            self.operation_mode_radios.append(radio)
        Tooltip(op_mode_frame, "CV: Voltage field is target, Current field is limit.\nCC: Current field is target, Voltage field is limit.")
        row += 1


        # Voltage
        ttk.Label(control_frame, text="Voltage (V):").grid(row=row, column=0, **grid_args)
        voltage_frame = ttk.Frame(control_frame, **FRAME_STYLE)
        voltage_frame.grid(row=row, column=1, **grid_args_entry)
        self.voltage_entry = ttk.Entry(voltage_frame, textvariable=self.voltage_var)
        self.voltage_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.voltage_entry, "In CV mode: Target Voltage.\nIn CC mode: Voltage Limit (OVP).") # Updated tooltip
        row += 1

        # Current
        ttk.Label(control_frame, text="Current (A):").grid(row=row, column=0, **grid_args)
        current_frame = ttk.Frame(control_frame, **FRAME_STYLE)
        current_frame.grid(row=row, column=1, **grid_args_entry)
        self.current_entry = ttk.Entry(current_frame, textvariable=self.current_var)
        self.current_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.current_entry, "In CV mode: Current Limit (OCP).\nIn CC mode: Target Current.") # Updated tooltip
        row += 1

        # Threshold
        ttk.Label(control_frame, text="Stop Threshold (A):").grid(row=row, column=0, **grid_args)
        threshold_frame = ttk.Frame(control_frame, **FRAME_STYLE)
        threshold_frame.grid(row=row, column=1, **grid_args_entry)
        self.threshold_entry = ttk.Entry(threshold_frame, textvariable=self.threshold_var)
        self.threshold_entry.pack(fill=tk.X, expand=True)
        row += 1

        # Save Location
        ttk.Label(control_frame, text="Save Location:").grid(row=row, column=0, **grid_args)
        save_location_frame = ttk.Frame(control_frame, **FRAME_STYLE)
        save_location_frame.grid(row=row, column=1, columnspan=2, **grid_args_entry)
        self.save_location_entry = ttk.Entry(save_location_frame, textvariable=self.save_location_var)
        self.save_location_entry.pack(fill=tk.X, expand=True)
        self.browse_button = ttk.Button(control_frame, text="Browse...", command=self.browse_save_location, width=10)
        self.browse_button.grid(row=row, column=3, **pad_args)
        row += 1

        # Export Format
        ttk.Label(control_frame, text="Export Format:").grid(row=row, column=0, **grid_args)
        format_frame = ttk.Frame(control_frame)
        format_frame.grid(row=row, column=1, columnspan=3, sticky=GRID_STICKY_W, padx=WIDGET_PADDING_X, pady=WIDGET_PADDING_Y) # Use sticky W
        self.export_format_radios: List[ttk.Radiobutton] = []
        export_options = VALID_EXPORT_FORMATS
        if not pd:
            export_options = ["csv"]
            self.add_status_message("Pandas not found. Excel/JSON export disabled.", STATUS_WARNING)
            if hasattr(self, 'export_format_var') and self.export_format_var.get() != "csv":
                 self.export_format_var.set("csv")

        for fmt in export_options:
            radio = ttk.Radiobutton(format_frame, text=fmt.upper(), variable=self.export_format_var, value=fmt)
            radio.pack(side=tk.LEFT, padx=(0, 10))
            self.export_format_radios.append(radio)
        row += 1

        # Configure column weights
        control_frame.columnconfigure(1, weight=1)
        control_frame.columnconfigure(2, weight=0) # Let frame containing entry expand
        control_frame.columnconfigure(3, weight=0) # Buttons fixed width

        # --- Electrochemical Cell Info Fields ---
        cell_row = 0
        # Anode
        ttk.Label(cell_info_frame, text="Anode:").grid(row=cell_row, column=0, **grid_args)
        anode_frame = ttk.Frame(cell_info_frame, **FRAME_STYLE)
        anode_frame.grid(row=cell_row, column=1, **grid_args_entry)
        self.anode_entry = ttk.Entry(anode_frame, textvariable=self.anode_var)
        self.anode_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.anode_entry, "Specify the material or description of the anode.")
        cell_row += 1
        # Cathode
        ttk.Label(cell_info_frame, text="Cathode:").grid(row=cell_row, column=0, **grid_args)
        cathode_frame = ttk.Frame(cell_info_frame, **FRAME_STYLE)
        cathode_frame.grid(row=cell_row, column=1, **grid_args_entry)
        self.cathode_entry = ttk.Entry(cathode_frame, textvariable=self.cathode_var)
        self.cathode_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.cathode_entry, "Specify the material or description of the cathode.")
        cell_row += 1
        # Electrolyte
        ttk.Label(cell_info_frame, text="Electrolyte:").grid(row=cell_row, column=0, **grid_args)
        electrolyte_frame = ttk.Frame(cell_info_frame, **FRAME_STYLE)
        electrolyte_frame.grid(row=cell_row, column=1, **grid_args_entry)
        self.electrolyte_entry = ttk.Entry(electrolyte_frame, textvariable=self.electrolyte_var)
        self.electrolyte_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.electrolyte_entry, "Specify the electrolyte used.")
        cell_row += 1
        # Electrolyte Molarity
        ttk.Label(cell_info_frame, text="Electrolyte Molarity (M):").grid(row=cell_row, column=0, **grid_args)
        molarity_frame = ttk.Frame(cell_info_frame, **FRAME_STYLE)
        molarity_frame.grid(row=cell_row, column=1, **grid_args_entry)
        self.electrolyte_molarity_entry = ttk.Entry(molarity_frame, textvariable=self.electrolyte_molarity_var)
        self.electrolyte_molarity_entry.pack(fill=tk.X, expand=True)
        Tooltip(self.electrolyte_molarity_entry, "Specify the molarity (concentration) of the electrolyte.")
        cell_row += 1
        cell_info_frame.columnconfigure(1, weight=1)

        # --- Status Indicator ---
        self.status_indicator = ttk.Label(status_frame, textvariable=self.status_var, anchor=tk.W, wraplength=STATUS_WRAPLENGTH)
        self.status_indicator.pack(fill=tk.X, padx=WIDGET_PADDING_X, pady=WIDGET_PADDING_Y)

        # --- Control Buttons ---
        self.start_button = ttk.Button(button_frame, text="Start Logging", command=self.start_logging)
        self.start_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)
        self.stop_button = ttk.Button(button_frame, text="Stop Logging", command=self.stop_logging, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=WIDGET_PADDING_X)


    def scan_visa_resources(self) -> None:
        """Scans for VISA resources and updates the combobox."""
        if not pyvisa:
            self.add_status_message("PyVISA not available. Cannot scan.", STATUS_ERROR)
            if hasattr(self, 'resource_combobox') and self.resource_combobox and self.resource_combobox.winfo_exists():
                self.resource_combobox['values'] = ["PyVISA Missing"]
                if hasattr(self, 'resource_var'):
                     self.resource_var.set("PyVISA Missing")
            return

        self.add_status_message("Scanning for VISA resources...", STATUS_INFO)
        if hasattr(self, 'scan_button') and self.scan_button and self.scan_button.winfo_exists():
             self.scan_button.config(state=tk.DISABLED)
        if hasattr(self, 'resource_combobox') and self.resource_combobox and self.resource_combobox.winfo_exists():
            self.resource_combobox.config(state=tk.DISABLED)

        self.executor.submit(self._scan_visa_task)

    def _scan_visa_task(self) -> None:
        """Task to scan for VISA resources in a background thread."""
        resources = []
        error_msg = None
        try:
            rm = pyvisa.ResourceManager()
            resources = rm.list_resources()
            rm.close()
        except pyvisa.errors.VisaIOError as e:
            error_msg = f"VISA Error during scan: {e}."
        except Exception as e:
            error_msg = f"Error during VISA scan: {e}"
            traceback.print_exc()

        if self.root.winfo_exists():
             self.root.after(0, self._update_visa_list, resources, error_msg)

    def _update_visa_list(self, resources: List[str], error_msg: Optional[str]) -> None:
        """Updates the resource combobox in the main GUI thread."""
        if not self.root.winfo_exists() or not hasattr(self, 'resource_combobox') or not self.resource_combobox or not self.resource_combobox.winfo_exists() or not hasattr(self, 'scan_button') or not self.scan_button or not self.scan_button.winfo_exists():
             return

        if error_msg:
            self.add_status_message(error_msg, STATUS_ERROR)
            self.resource_combobox['values'] = ["Scan Error"]
            if hasattr(self, 'resource_var'):
                 self.resource_var.set("Scan Error")
        elif resources:
            self.resource_combobox['values'] = list(resources)
            if hasattr(self, 'resource_var'):
                current_resource = self.resource_var.get()
                if current_resource not in resources and resources: # Select first if current not valid and list not empty
                    self.resource_var.set(resources[0])
            self.add_status_message(f"Found {len(resources)} VISA resource(s).", STATUS_INFO)
        else:
            self.resource_combobox['values'] = ["No Resources Found"]
            if hasattr(self, 'resource_var'):
                self.resource_var.set("No Resources Found")
            self.add_status_message("No VISA resources found.", STATUS_WARNING)

        self.scan_button.config(state=tk.NORMAL)
        # Re-enable combobox only if NOT simulating AND pyvisa is available
        if hasattr(self, 'simulation_mode_var') and not self.simulation_mode_var.get() and pyvisa:
             self.resource_combobox.config(state="readonly")
        else:
             self.resource_combobox.config(state=tk.DISABLED) # Keep disabled if simulating or no pyvisa


    def _setup_plot_tab(self) -> None:
        """Set up the plot tab UI (canvas and toolbar are created elsewhere)."""
        if not hasattr(self, 'plot_tab') or not self.plot_tab or not self.plot_tab.winfo_exists():
             return

        # Add a button to save the plot (now also saves/clears data)
        save_button = ttk.Button(self.plot_tab, text="Save Plot Image (& Data)", command=self.save_plot_image)
        Tooltip(save_button, "Save the current plot image.\nThis will also save the corresponding dataset and clear the log.")
        save_button.pack(side=tk.BOTTOM, pady=5) # Place at bottom below toolbar

    def _create_plot_canvas(self) -> None:
        """Creates the Matplotlib canvas."""
        if not hasattr(self, 'fig') or not self.fig: return
        if hasattr(self, 'canvas_widget') and self.canvas_widget and self.canvas_widget.winfo_exists():
            self.canvas_widget.destroy()
            self.canvas = None
            self.canvas_widget = None

        if not hasattr(self, 'plot_tab') or not self.plot_tab or not self.plot_tab.winfo_exists():
             return

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.plot_tab)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    def _create_plot_toolbar(self) -> None:
        """Creates or updates the Matplotlib navigation toolbar."""
        if not hasattr(self, 'canvas') or not self.canvas or not hasattr(self, 'plot_tab') or not self.plot_tab or not self.plot_tab.winfo_exists(): return
        for widget in self.plot_tab.winfo_children():
            if isinstance(widget, NavigationToolbar2Tk):
                widget.destroy()
                break

        toolbar = NavigationToolbar2Tk(self.canvas, self.plot_tab)
        toolbar.update()
        toolbar.pack(side=tk.BOTTOM, fill=tk.X)

    def save_plot_image(self) -> None:
        """Save the current plot view as an image file, and save/clear data."""
        if not hasattr(self, 'fig') or not self.fig:
            self.add_status_message("No plot figure available to save.", STATUS_WARNING)
            return
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_dir = self.config_manager.config.get(CONFIG_SAVE_LOCATION, os.getcwd())
            default_filename = f"power_plot_{timestamp}.png"

            if not self.root.winfo_exists():
                 self.add_status_message("Cannot save plot image: Main window is closed.", STATUS_ERROR)
                 return

            filepath = filedialog.asksaveasfilename(
                title="Save Plot Image As",
                initialdir=save_dir,
                initialfile=default_filename,
                defaultextension=".png",
                filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"), ("All files", "*.*")]
            )

            if filepath:
                # 1. Save the plot image first
                self.fig.savefig(filepath)
                self.add_status_message(f"Plot saved as '{os.path.basename(filepath)}'", STATUS_SUCCESS)

                # 2. Prepare to save dataset
                base_filepath, _ = os.path.splitext(filepath) # Get filename without extension
                export_format = self.export_format_var.get()
                notes_content = ""
                try:
                    if hasattr(self, 'notes_text') and self.notes_text and self.notes_text.winfo_exists():
                        notes_content = self.notes_text.get("1.0", tk.END).strip()
                except Exception as e:
                    self.add_status_message(f"Warning: Could not get notes content for data export: {e}", STATUS_WARNING)

                # 3. Check if data exists in logger
                data_to_save = []
                headers_to_save = []
                config_snapshot = {}
                logger_exists = hasattr(self, 'logger') and self.logger
                data_exists_in_logger = logger_exists and hasattr(self.logger, '_logged_data') and self.logger._logged_data

                if data_exists_in_logger:
                    # Copy data and config BEFORE clearing
                    data_to_save = list(self.logger._logged_data) # Important: Copy!
                    if hasattr(self.logger, '_data_headers'):
                        headers_to_save = list(self.logger._data_headers)
                    if hasattr(self, 'config_manager') and self.config_manager:
                         config_snapshot = dict(self.config_manager.config) # Snapshot of config

                    # 4. Save the dataset using static methods
                    self.add_status_message(f"Saving dataset corresponding to plot '{os.path.basename(filepath)}'...", STATUS_INFO)
                    save_triggered = False

                    # Save CSV
                    if export_format in ["csv", "all"]:
                        save_triggered = True
                        csv_filename = f"{base_filepath}.csv"
                        DataLogger.save_data_to_csv_static(
                            csv_filename, data_to_save, headers_to_save, config_snapshot, notes_content,
                            self.status_queue, self.error_queue
                        )

                    # Save Excel (if pandas/openpyxl available)
                    if export_format in ["xlsx", "all"]:
                        if pd and openpyxl:
                            save_triggered = True
                            excel_filename = f"{base_filepath}.xlsx"
                            DataLogger.save_data_to_excel_static(
                                excel_filename, data_to_save, headers_to_save, config_snapshot, notes_content,
                                self.status_queue, self.error_queue
                            )
                        elif export_format == "xlsx": # Warn only if specifically selected
                             self.add_status_message("Pandas/Openpyxl missing. Cannot save dataset as XLSX.", STATUS_WARNING)
                        # No warning if 'all' and pandas missing, as CSV still saves

                    # Save JSON (if pandas available)
                    if export_format in ["json", "all"]:
                         if pd:
                             save_triggered = True
                             json_filename = f"{base_filepath}.json"
                             DataLogger.save_data_to_json_static(
                                 json_filename, data_to_save, headers_to_save, config_snapshot, notes_content,
                                 self.status_queue, self.error_queue
                             )
                         elif export_format == "json": # Warn only if specifically selected
                             self.add_status_message("Pandas missing. Cannot save dataset as JSON.", STATUS_WARNING)
                         # No warning if 'all' and pandas missing, as CSV still saves

                    # 5. Clear the data if save was attempted
                    if save_triggered:
                        if hasattr(self.logger, '_logged_data'):
                             self.logger._logged_data.clear() # Clear logger's internal full log
                        if hasattr(self, 'data_manager'):
                             self.data_manager.clear_plot_data() # Clear data used for plotting

                        # 6. Refresh plot visually to show cleared data
                        if hasattr(self, 'canvas') and self.canvas and self.canvas.get_tk_widget().winfo_exists():
                            try:
                                # Update lines to empty
                                if hasattr(self, 'line_v') and self.line_v: self.line_v.set_data([], [])
                                if hasattr(self, 'line_i') and self.line_i: self.line_i.set_data([], [])
                                if hasattr(self, 'line_p') and self.line_p: self.line_p.set_data([], [])
                                if hasattr(self, 'line_r') and self.line_r: self.line_r.set_data([], [])
                                self._update_plot_axes() # Rescale axes to default empty state
                                self.canvas.draw_idle()
                                self.add_status_message("Logged data cleared.", STATUS_INFO)
                            except Exception as e:
                                self.add_status_message(f"Error refreshing plot after clearing data: {e}", STATUS_WARNING)
                                traceback.print_exc()
                    # else: No save was triggered (e.g., export format invalid and pandas missing)

                else:
                    # Logger doesn't exist or has no data
                    self.add_status_message("No logged data available to save or clear.", STATUS_INFO)

        except Exception as e:
            self.add_status_message(f"Error during plot/data save process: {e}", STATUS_ERROR)
            traceback.print_exc()

    def _create_plot_figure(self) -> None:
        """Creates the matplotlib figure and axes based on the current plot style."""
        if hasattr(self, 'fig') and self.fig:
             plt.close(self.fig) # Close previous figure
             self.fig = None # Clear the reference

        plot_style = self.config_manager.config.get("PLOT_STYLE", DEFAULT_PLOT_STYLE) # Use get with default

        try:
            # Use context for temporary style change
            with plt.style.context(plot_style):
                self.fig, axes = plt.subplots(4, 1, sharex=True, figsize=(8, 7)) # Adjusted figsize
                self.ax_v, self.ax_i, self.ax_p, self.ax_r = axes # Unpack axes

                # Set labels
                self.ax_v.set_ylabel("Voltage (V)")
                self.ax_i.set_ylabel("Current (A)")
                self.ax_p.set_ylabel("Power (W)")
                self.ax_r.set_ylabel("Resistance (Ω)")
                self.ax_r.set_xlabel("Time (s)")

                # Add grids
                for ax in axes:
                    ax.grid(True, linestyle=':', alpha=0.9, linewidth=1.0)
                # Improve layout
                self.fig.tight_layout(pad=1.5, rect=[0, 0.03, 1, 0.97]) # Adjust padding/rect

        except Exception as e:
             print(f"Error applying plot style '{plot_style}': {e}")
             self.add_status_message(f"Error applying plot style '{plot_style}': {e}", STATUS_ERROR)
             # Fallback to default if style fails
             try:
                 plt.style.use("default")
                 self.fig, axes = plt.subplots(4, 1, sharex=True, figsize=(8, 7))
                 self.ax_v, self.ax_i, self.ax_p, self.ax_r = axes
                 # Set labels and grids again for fallback
                 self.ax_v.set_ylabel("Voltage (V)")
                 self.ax_i.set_ylabel("Current (A)")
                 self.ax_p.set_ylabel("Power (W)")
                 self.ax_r.set_ylabel("Resistance (Ω)")
                 self.ax_r.set_xlabel("Time (s)")
                 for ax in axes: ax.grid(True, linestyle=':')
                 self.fig.tight_layout(pad=1.5, rect=[0, 0.03, 1, 0.97])
             except Exception as fallback_e:
                 print(f"Critical error during plot style fallback: {fallback_e}")
                 self.fig = None # Ensure fig is None if fallback fails
                 self.ax_v, self.ax_i, self.ax_p, self.ax_r = None, None, None, None
                 self.add_status_message(f"Critical error setting up plot figure: {fallback_e}", STATUS_ERROR)


    def _create_plot_lines(self) -> None: # Added self here
        """Creates the line artists for the plot axes."""
        if not hasattr(self, 'ax_v') or not self.ax_v or not hasattr(self, 'ax_i') or not self.ax_i or not hasattr(self, 'ax_p') or not self.ax_p or not hasattr(self, 'ax_r') or not self.ax_r:
            # Ensure all axes exist before creating lines
             self.line_v, self.line_i, self.line_p, self.line_r = None, None, None, None
             return # Ensure axes exist

        # Define line colors (can be themed later if desired)
        self.line_v, = self.ax_v.plot([], [], 'b-', label="Voltage", linewidth=1.5)
        self.line_i, = self.ax_i.plot([], [], 'r-', label="Current", linewidth=1.5)
        self.line_p, = self.ax_p.plot([], [], 'g-', label="Power", linewidth=1.5)
        self.line_r, = self.ax_r.plot([], [], 'c-', label="Resistance", linewidth=1.5)

        # Add legends
        for ax in [self.ax_v, self.ax_i, self.ax_p, self.ax_r]:
             ax.legend(loc='upper right', fontsize='small')


    def _apply_plot_style(self, style_name: str) -> None:
        """Applies the selected matplotlib plot style and redraws."""
        if self.config_manager:
             self.config_manager.config["PLOT_STYLE"] = style_name
        try:
            self._create_plot_figure() # Recreate figure with new style
            self._create_plot_lines()  # Recreate lines on new axes
            self._create_plot_canvas() # Recreate canvas with new figure
            self._create_plot_toolbar()# Recreate toolbar for new canvas
            # Check if canvas exists before drawing
            if hasattr(self, 'canvas') and self.canvas:
                self.canvas.draw_idle()   # Redraw

            # Restart animation if it was running and figure/canvas are ready
            # Check if ani exists and has an event_source before calling is_running
            if self.is_logging and hasattr(self, 'ani') and self.ani and hasattr(self.ani, 'event_source') and self.ani.event_source and self.ani.event_source.is_running() and hasattr(self, 'fig') and self.fig and hasattr(self, 'canvas') and self.canvas and self.canvas.get_tk_widget().winfo_exists():
                 # If logging and animation was running, stop the old one before starting a new one
                 try:
                      self.ani.event_source.stop()
                 except Exception as stop_e:
                      print(f"Minor error stopping old animation: {stop_e}")

                 # Start new animation
                 self.start_animation()


        except Exception as e:
             print(f"Error during _apply_plot_style: {e}")
             self.add_status_message(f"Error applying plot style '{style_name}': {e}", STATUS_ERROR)
             # Attempt to fallback or ensure app doesn't crash


    def _apply_theme(self, theme_name: str) -> None:
        """Applies the selected GUI theme, optionally with fade."""
        # Check if the root is an instance of the actual ThemedTk from ttkthemes
        if not isinstance(self.root, ThemedTk) or not hasattr(self.root, 'set_theme'):
            self.add_status_message("ttkthemes not available or root is not a ThemedTk instance. Cannot apply theme.", STATUS_WARNING)
            return # Cannot apply theme if not using ThemedTk or if set_theme is missing

        # Check if theme name is valid (basic check)
        if theme_name not in self.root.get_themes():
            self.add_status_message(f"Invalid theme name: '{theme_name}'. Using current theme.", STATUS_WARNING)
            return # Don't apply invalid theme

        # Check if the root window still exists
        if not self.root.winfo_exists():
            print("Root window closed. Cannot apply theme.")
            return

        try:
            enable_fade = self.enable_theme_fade_var.get() if hasattr(self, 'enable_theme_fade_var') else DEFAULT_ENABLE_THEME_FADE

            # Safely get current_theme, checking if get_theme_names exists
            current_theme = None
            if hasattr(self.root, 'get_theme_names'): # ttkthemes ThemedTk might not have get_theme_names
                try: # ThemedTk might use theme_names() or get_themes()
                    current_theme = self.root.theme_use() # Standard ttk way
                except: # Fallback if theme_use isn't what we need or fails
                    current_themes = self.root.get_themes()
                    if current_themes: # Ensure it's not empty
                         # This part is a bit tricky as ttkthemes might not directly tell you the *current* one easily
                         # For now, we assume if fade is on, we do it.
                         pass


            if enable_fade and current_theme and current_theme != theme_name: # and current_theme can be tricky with ttkthemes
                # Use faster fade for theme changes
                self._fade_window(direction="out", steps=FAST_FADE_STEPS, duration_ms=FAST_FADE_DURATION_MS, callback=lambda: self._change_theme_and_fade_in(theme_name))
            else:
                # No fade, just change theme directly
                self._change_theme_and_fade_in(theme_name, fade_in=False) # Call the same logic without fade-out

        except Exception as e:
            self.add_status_message(f"Error during theme change initiation: {e}", STATUS_ERROR)
            traceback.print_exc()
            # Attempt to change theme directly if fade initiation fails
            try:
                self._change_theme_and_fade_in(theme_name, fade_in=False)
            except Exception as direct_e:
                print(f"Error applying theme directly after fade fail: {direct_e}")
                self.add_status_message(f"Critical error applying theme: {direct_e}", STATUS_ERROR)

    def _change_theme_and_fade_in(self, theme_name: str, fade_in: bool = True) -> None:
            """Changes the theme and optionally fades the window back in."""
            try:
                # Check if the root window still exists
                if not self.root.winfo_exists():
                    print("Root window closed. Cannot change theme.")
                    return

                self.root.set_theme(theme_name)
                self.add_status_message(f"GUI theme changed to '{theme_name}'", STATUS_INFO)

                # Re-create plot elements to match the new theme's styling (colors, fonts etc.)
                self._redraw_plot_for_theme()

                # Save the new theme to config AFTER successfully applying it
                if hasattr(self, 'config_manager') and self.config_manager:
                    self.config_manager.config["GUI_THEME"] = theme_name
                    self.config_manager.save_config() # Save config after theme change

                if fade_in:
                    self._fade_window(direction="in", steps=FAST_FADE_STEPS, duration_ms=FAST_FADE_DURATION_MS)
                else:
                    # Ensure alpha is 1.0 if no fade-in happens
                    if self.root.winfo_exists():
                        self.root.attributes('-alpha', 1.0)

            except Exception as e:
                self.add_status_message(f"Error applying theme '{theme_name}': {e}", STATUS_ERROR)
                traceback.print_exc()
                # Attempt to revert to a safe default theme or the previous one if possible
                try:
                    if isinstance(self.root, ThemedTk):
                        self.root.set_theme("clam") # Fallback theme
                        self._redraw_plot_for_theme() # Redraw plots for fallback
                        self.add_status_message("Attempted to revert to 'clam' theme due to error.", STATUS_WARNING)
                except Exception as fallback_e:
                    print(f"Error during theme change fallback: {fallback_e}")
                    self.add_status_message(f"Critical error applying fallback theme: {fallback_e}", STATUS_ERROR)


    def _redraw_plot_for_theme(self) -> None:
        """Forces recreation/redraw of plot elements sensitive to themes."""
        # Check if root window exists before redrawing plot
        if not self.root.winfo_exists():
             return

        # Re-apply the current plot style, which recreates figure/axes/lines/canvas
        current_plot_style = self.config_manager.config.get("PLOT_STYLE", DEFAULT_PLOT_STYLE) # Use get with default
        self._apply_plot_style(current_plot_style)


    def on_gui_theme_selected(self, event=None) -> None:
        """Event handler for GUI theme combobox selection."""
        # Check if combobox widget exists
        if not hasattr(self, 'gui_theme_combobox') or not self.gui_theme_combobox or not self.gui_theme_combobox.winfo_exists():
             return

        selected_theme = self.gui_theme_combobox.get()
        if selected_theme and hasattr(self, 'config_manager') and self.config_manager and selected_theme != self.config_manager.config.get("GUI_THEME", DEFAULT_GUI_THEME):
            self._apply_theme(selected_theme)
            # Configuration is saved within _apply_theme after successful change
            # self.apply_and_save_config() # Moved saving to _apply_theme or _change_theme_and_fade_in
    def on_plot_style_selected(self, event=None) -> None:
        """Event handler for Plot style combobox selection."""
        # Check if combobox widget exists
        if not hasattr(self, 'plot_style_combobox') or not self.plot_style_combobox or not self.plot_style_combobox.winfo_exists():
             return

        selected_style = self.plot_style_combobox.get()
        if selected_style and hasattr(self, 'config_manager') and self.config_manager and selected_style != self.config_manager.config.get("PLOT_STYLE", DEFAULT_PLOT_STYLE):
             self._apply_plot_style(selected_style)
             self.apply_and_save_config() # Save style change immediately

    def update_ui_state(self, logging_active: bool) -> None:
        """Enables or disables UI elements based on logging state."""
        # Check if root window exists
        if not self.root.winfo_exists():
             return

        self.is_logging = logging_active
        state = tk.DISABLED if logging_active else tk.NORMAL
        readonly_state = tk.DISABLED if logging_active else "readonly"

        # Control Tab elements - Check if widgets exist before configuring
        if hasattr(self, 'voltage_entry') and self.voltage_entry and self.voltage_entry.winfo_exists(): self.voltage_entry.config(state=state)
        if hasattr(self, 'current_entry') and self.current_entry and self.current_entry.winfo_exists(): self.current_entry.config(state=state)
        if hasattr(self, 'threshold_entry') and self.threshold_entry and self.threshold_entry.winfo_exists(): self.threshold_entry.config(state=state)
        if hasattr(self, 'save_location_entry') and self.save_location_entry and self.save_location_entry.winfo_exists(): self.save_location_entry.config(state=state)
        if hasattr(self, 'browse_button') and self.browse_button and self.browse_button.winfo_exists(): self.browse_button.config(state=state)

        for radio in self.export_format_radios:
            if radio.winfo_exists(): radio.config(state=state)
        for radio in self.operation_mode_radios: # New
            if radio.winfo_exists(): radio.config(state=state)


        # Resource selection - disable only if logging
        # Re-evaluated logic for simulation mode vs. pyvisa presence
        resource_control_state = tk.DISABLED # Default to disabled
        scan_button_state = tk.DISABLED

        if not logging_active: # Only enable if not logging
            if hasattr(self, 'simulation_mode_var') and not self.simulation_mode_var.get() and pyvisa: # Enable if not simulating AND pyvisa is available
                resource_control_state = "readonly"
                scan_button_state = tk.NORMAL
            # else: keep disabled if simulating or pyvisa is missing

        if hasattr(self, 'resource_combobox') and self.resource_combobox and self.resource_combobox.winfo_exists():
             self.resource_combobox.config(state=resource_control_state)
        if hasattr(self, 'scan_button') and self.scan_button and self.scan_button.winfo_exists():
            self.scan_button.config(state=scan_button_state)


        # Settings Tab elements - Check if widgets exist before configuring
        if hasattr(self, 'update_interval_entry') and self.update_interval_entry and self.update_interval_entry.winfo_exists(): self.update_interval_entry.config(state=state)
        if hasattr(self, 'max_plot_points_entry') and self.max_plot_points_entry and self.max_plot_points_entry.winfo_exists(): self.max_plot_points_entry.config(state=state)
        if hasattr(self, 'gui_theme_combobox') and self.gui_theme_combobox and self.gui_theme_combobox.winfo_exists(): self.gui_theme_combobox.config(state=readonly_state)
        if hasattr(self, 'plot_style_combobox') and self.plot_style_combobox and self.plot_style_combobox.winfo_exists(): self.plot_style_combobox.config(state=readonly_state)
        if hasattr(self, 'preset_name_entry') and self.preset_name_entry and self.preset_name_entry.winfo_exists(): self.preset_name_entry.config(state=state)
        if hasattr(self, 'save_preset_button') and self.save_preset_button and self.save_preset_button.winfo_exists(): self.save_preset_button.config(state=state)
        if hasattr(self, 'delete_preset_button') and self.delete_preset_button and self.delete_preset_button.winfo_exists(): self.delete_preset_button.config(state=state)
        if hasattr(self, 'load_preset_button') and self.load_preset_button and self.load_preset_button.winfo_exists(): self.load_preset_button.config(state=state)
         # Check if radio buttons for stop condition exist
        if hasattr(self, 'radio_stop_below') and self.radio_stop_below and self.radio_stop_below.winfo_exists(): self.radio_stop_below.config(state=state)
        if hasattr(self, 'radio_stop_above') and self.radio_stop_above and self.radio_stop_above.winfo_exists(): self.radio_stop_above.config(state=state)
        # Listbox state needs special handling depending on whether there are presets
        if hasattr(self, 'preset_listbox') and self.preset_listbox and self.preset_listbox.winfo_exists():
             if hasattr(self, 'config_manager') and self.config_manager and self.config_manager.get_preset_names() and not logging_active:
                  self.preset_listbox.config(state=tk.NORMAL)
             else:
                  self.preset_listbox.config(state=tk.DISABLED)
        if hasattr(self, 'simulation_mode_check') and self.simulation_mode_check and self.simulation_mode_check.winfo_exists(): self.simulation_mode_check.config(state=state) # Allow changing simulation only when stopped

        # Notes tab text widget state (should always be editable unless logging is active)
        # We want notes to be editable even if not logging, so state depends only on logging_active
        notes_state = tk.DISABLED if logging_active else tk.NORMAL
        if hasattr(self, 'notes_text') and self.notes_text and self.notes_text.winfo_exists():
            self.notes_text.config(state=notes_state)

        # Start/Stop Buttons - Check if widgets exist before configuring
        if hasattr(self, 'start_button') and self.start_button and self.start_button.winfo_exists(): self.start_button.config(state=tk.DISABLED if logging_active else tk.NORMAL)
        if hasattr(self, 'stop_button') and self.stop_button and self.stop_button.winfo_exists(): self.stop_button.config(state=tk.NORMAL if logging_active else tk.DISABLED)

    def browse_save_location(self) -> None:
        """Opens a dialog to select the save directory."""
        # Check if root window exists for dialog
        if not self.root.winfo_exists():
             self.add_status_message("Cannot browse: Main window is closed.", STATUS_ERROR)
             return

        initial_dir = self.save_location_var.get()
        if not os.path.isdir(initial_dir) and hasattr(self, 'config_manager') and self.config_manager:
             initial_dir = self.config_manager.base_path

        save_location = filedialog.askdirectory(title="Select Save Location", initialdir=initial_dir)
        if save_location:
            if hasattr(self, 'save_location_var'):
                 self.save_location_var.set(save_location)

    def apply_and_save_config(self) -> None:
        """Validates UI settings, updates config, and saves to file."""
        # Check if root window exists before proceeding
        if not self.root.winfo_exists():
             self.add_status_message("Cannot apply/save settings: Main window is closed.", STATUS_ERROR)
             return

        try:
            # --- Validate and Get Values ---
            resource_name = self.resource_var.get().strip()
            # Ensure voltage and current are not negative, threshold can be 0
            voltage = self._validate_float_input(self.voltage_var.get(), "Voltage", allow_zero=False, allow_negative=False)
            current = self._validate_float_input(self.current_var.get(), "Current", allow_zero=False, allow_negative=False) # Current limit should likely be > 0
            threshold = self._validate_float_input(self.threshold_var.get(), "Threshold", allow_zero=True, allow_negative=False)
            stop_condition = self.stop_condition_var.get() # Get the new setting
            update_interval = self._validate_int_input(self.update_interval_var.get(), "Plot Update Interval", min_val=10) # Min 10ms
            max_plot_points = self._validate_int_input(self.max_plot_points_var.get(), "Max Plot Data Points", min_val=10) # Min 10 points
            save_location = self.save_location_var.get().strip()
            export_format = self.export_format_var.get()
            gui_theme = self.gui_theme_var.get()
            plot_style = self.plot_style_var.get()
            simulation_mode = self.simulation_mode_var.get()
            operation_mode = self.operation_mode_var.get() # New


            if not save_location or not os.path.isdir(save_location):
                 # Attempt to create the directory if it doesn't exist
                 try:
                     os.makedirs(save_location, exist_ok=True)
                     self.add_status_message(f"Created save location directory: {save_location}", STATUS_INFO)
                 except OSError as e:
                      raise ValueError(f"Invalid or non-existent Save Location directory and failed to create it: {e}")

            if not pd and export_format != 'csv':
                self.add_status_message(f"Pandas missing, forcing export format to CSV.", STATUS_WARNING)
                export_format = 'csv'
                # Check if radio buttons exist before setting
                if self.export_format_radios:
                     self.export_format_var.set('csv') # Update UI

            # --- Update Config Dictionary ---
            if hasattr(self, 'config_manager') and self.config_manager:
                self.config_manager.config[CONFIG_RESOURCE_NAME] = resource_name
                self.config_manager.config[CONFIG_VOLTAGE] = voltage
                self.config_manager.config[CONFIG_CURRENT] = current
                self.config_manager.config[CONFIG_THRESHOLD] = threshold
                self.config_manager.config[CONFIG_STOP_CONDITION] = stop_condition # Save the new setting
                self.config_manager.config[CONFIG_SAVE_LOCATION] = save_location
                self.config_manager.config[CONFIG_EXPORT_FORMAT] = export_format
                self.config_manager.config[CONFIG_UPDATE_INTERVAL] = update_interval
                self.config_manager.config[CONFIG_MAX_PLOT_POINTS] = max_plot_points
                self.config_manager.config[CONFIG_SIMULATION_MODE] = simulation_mode
                self.config_manager.config[CONFIG_OPERATION_MODE] = operation_mode # New
                self.config_manager.config[CONFIG_ANODE] = self.anode_var.get().strip()
                self.config_manager.config[CONFIG_CATHODE] = self.cathode_var.get().strip()
                self.config_manager.config[CONFIG_ELECTROLYTE] = self.electrolyte_var.get().strip()
                self.config_manager.config[CONFIG_ELECTROLYTE_MOLARITY] = self.electrolyte_molarity_var.get().strip()


                # --- Save Config File ---
                self.config_manager.save_config()


            # --- Update Application State ---
            # Re-create DataManager if max_plot_points changed
            # This should ideally only happen when NOT logging
            if not self.is_logging and hasattr(self, 'data_manager') and self.data_manager and hasattr(self.data_manager, 'max_plot_points') and self.data_manager.max_plot_points != max_plot_points:
                self.data_manager = DataManager(max_plot_points=max_plot_points)
                self.add_status_message(f"Max plot points updated to {max_plot_points}.", STATUS_INFO)


            # Animation interval change applies on next Start, no need to force restart here
            # Plot style change is handled by on_plot_style_selected

            self.add_status_message("Settings applied and configuration saved.", STATUS_SUCCESS)

        except ValueError as e:
            messagebox.showerror("Input Validation Error", str(e))
        except Exception as e:
            messagebox.showerror("Configuration Error", f"Error applying and saving configuration: {e}")
            traceback.print_exc()


    def save_preset(self) -> None:
        """Saves current control settings as a named preset."""
        # Check if root window exists
        if not self.root.winfo_exists():
             self.add_status_message("Cannot save preset: Main window is closed.", STATUS_ERROR)
             return
        # Check if preset_name_var exists
        if not hasattr(self, 'preset_name_var'):
            self.add_status_message("Cannot save preset: Preset name variable not initialized.", STATUS_ERROR)
            return

        name = self.preset_name_var.get().strip()
        if not name:
            messagebox.showwarning("Input Error", "Please enter a preset name.")
            return

        try:
            # Validate inputs before saving preset
            # Allow zero current/voltage in preset? Maybe, depends on use case.
            # For now, use the same validation as start logging.
            voltage = self._validate_float_input(self.voltage_var.get(), "Voltage", allow_zero=False, allow_negative=False)
            current = self._validate_float_input(self.current_var.get(), "Current", allow_zero=False, allow_negative=False)
            threshold = self._validate_float_input(self.threshold_var.get(), "Threshold", allow_zero=True, allow_negative=False)
            stop_condition = self.stop_condition_var.get() # Save the new setting

            preset_data = {
                CONFIG_RESOURCE_NAME: self.resource_var.get(), # Save selected resource
                CONFIG_VOLTAGE: voltage,
                CONFIG_CURRENT: current,
                CONFIG_THRESHOLD: threshold,
                CONFIG_STOP_CONDITION: stop_condition, # Save the new setting
                CONFIG_EXPORT_FORMAT: self.export_format_var.get(),
                CONFIG_OPERATION_MODE: self.operation_mode_var.get(), # New
                CONFIG_ANODE: self.anode_var.get(), # New
                CONFIG_CATHODE: self.cathode_var.get(), # New
                CONFIG_ELECTROLYTE: self.electrolyte_var.get(), # New
                CONFIG_ELECTROLYTE_MOLARITY: self.electrolyte_molarity_var.get() # New
            }

            if hasattr(self, 'config_manager') and self.config_manager and self.config_manager.add_preset(name, preset_data):
                self.load_presets_listbox() # Refresh listbox
                # Select the newly saved/updated preset in the listbox
                try:
                     # Check if listbox exists before selecting
                     if hasattr(self, 'preset_listbox') and self.preset_listbox and self.preset_listbox.winfo_exists():
                         idx = self.config_manager.get_preset_names().index(name)
                         self.preset_listbox.selection_clear(0, tk.END)
                         self.preset_listbox.selection_set(idx)
                         self.preset_listbox.activate(idx)
                         self.preset_listbox.see(idx)
                except ValueError:
                    pass # Should not happen if name is in list
                self.add_status_message(f"Preset '{name}' saved.", STATUS_SUCCESS)
            # else: # add_preset now always returns True if name provided
            #      self.add_status_message(f"Failed to save preset '{name}'.", STATUS_WARNING)

        except ValueError as e:
             messagebox.showerror("Input Validation Error", str(e))
        except Exception as e:
            self.add_status_message(f"Error saving preset: {e}", STATUS_ERROR)
            traceback.print_exc()

    def load_presets_listbox(self) -> None:
        """Populates the preset listbox from the config manager."""
        # Check if listbox widget exists
        if not hasattr(self, 'preset_listbox') or not self.preset_listbox or not self.preset_listbox.winfo_exists():
             return

        try:
            # Get current selection before clearing
            current_selection = self.preset_listbox.curselection()
            selected_name = self.preset_listbox.get(current_selection[0]) if current_selection else None

            self.preset_listbox.delete(0, tk.END)
            if hasattr(self, 'config_manager') and self.config_manager:
                preset_names = self.config_manager.get_preset_names()
                for name in preset_names:
                    self.preset_listbox.insert(tk.END, name)

                # Restore selection if possible
                if selected_name and selected_name in preset_names:
                    try:
                        idx = preset_names.index(selected_name)
                        self.preset_listbox.selection_set(idx)
                        self.preset_listbox.activate(idx)
                        self.preset_listbox.see(idx)
                    except ValueError:
                        pass # Should not happen if name was found

            # Update listbox state based on logging status
            # Need to check if the root window exists before calling update_ui_state
            if self.root.winfo_exists():
                self.update_ui_state(self.is_logging)

        except Exception as e:
            self.add_status_message(f"Error loading presets list: {e}", STATUS_ERROR)

    def load_preset(self) -> None:
        """Loads the selected preset's values into the control UI."""
        # Check if listbox widget exists
        if not hasattr(self, 'preset_listbox') or not self.preset_listbox or not self.preset_listbox.winfo_exists():
             self.add_status_message("Cannot load preset: Presets list not available.", STATUS_ERROR)
             return

        selection = self.preset_listbox.curselection()
        if not selection:
            messagebox.showinfo("Selection Required", "Please select a preset from the list to load.")
            return
        if self.is_logging:
             messagebox.showwarning("Logging Active", "Cannot load preset while logging is active.")
             return
         # Check if root window exists for messagebox
        if not self.root.winfo_exists():
             self.add_status_message("Cannot load preset: Main window is closed.", STATUS_ERROR)
             return


        try:
            preset_name = self.preset_listbox.get(selection[0])
            if hasattr(self, 'config_manager') and self.config_manager:
                preset = self.config_manager.get_preset(preset_name)
            else:
                 self.add_status_message("Config manager not available.", STATUS_ERROR)
                 return

            if preset:
                # Update resource variable and combobox
                resource_name = preset.get(CONFIG_RESOURCE_NAME, "") # Default to "" if missing
                # Check if combobox exists before setting value/values
                if hasattr(self, 'resource_combobox') and self.resource_combobox and self.resource_combobox.winfo_exists():
                     if hasattr(self, 'resource_var'):
                         self.resource_var.set(resource_name)
                     # Ensure the loaded resource is visible in the combobox values
                     # (Scan should ideally be run first if resource isn't listed)
                     current_values = list(self.resource_combobox['values'])
                     if resource_name and resource_name not in current_values and "Missing" not in resource_name and "Error" not in resource_name and "None" not in resource_name:
                         # Add it temporarily if not found (might be offline)
                         # Ensure the listbox values are updated in the UI after potentially adding
                         updated_values = sorted(list(set(current_values + [resource_name]))) # Use set to avoid duplicates
                         self.resource_combobox['values'] = updated_values
                         # Re-set the value after updating values list
                         if hasattr(self, 'resource_var'):
                             self.resource_var.set(resource_name)


                # Update other control variables - Check if entries exist before setting
                if hasattr(self, 'voltage_var') and self.voltage_var and hasattr(self, 'voltage_entry') and self.voltage_entry and self.voltage_entry.winfo_exists():
                     self.voltage_var.set(str(preset.get(CONFIG_VOLTAGE, self.config_manager.config.get(CONFIG_VOLTAGE, 0.0))))
                if hasattr(self, 'current_var') and self.current_var and hasattr(self, 'current_entry') and self.current_entry and self.current_entry.winfo_exists():
                     self.current_var.set(str(preset.get(CONFIG_CURRENT, self.config_manager.config.get(CONFIG_CURRENT, 0.0))))
                if hasattr(self, 'threshold_var') and self.threshold_var and hasattr(self, 'threshold_entry') and self.threshold_entry and self.threshold_entry.winfo_exists():
                     self.threshold_var.set(str(preset.get(CONFIG_THRESHOLD, self.config_manager.config.get(CONFIG_THRESHOLD, 0.0))))
                 # Load the new setting
                if hasattr(self, 'stop_condition_var') and self.stop_condition_var:
                     self.stop_condition_var.set(preset.get(CONFIG_STOP_CONDITION, self.config_manager.config.get(CONFIG_STOP_CONDITION, "below")))


                # Ensure export format is valid (in case pandas was missing when preset saved)
                loaded_format = preset.get(CONFIG_EXPORT_FORMAT, self.config_manager.config.get(CONFIG_EXPORT_FORMAT, "csv"))
                if not pd and loaded_format != 'csv':
                     loaded_format = 'csv'
                if hasattr(self, 'export_format_var'):
                     self.export_format_var.set(loaded_format)

                if hasattr(self, 'operation_mode_var'): # New
                    self.operation_mode_var.set(preset.get(CONFIG_OPERATION_MODE, MODE_CONSTANT_VOLTAGE))

                if hasattr(self, 'anode_var'): self.anode_var.set(preset.get(CONFIG_ANODE, "")) # New
                if hasattr(self, 'cathode_var'): self.cathode_var.set(preset.get(CONFIG_CATHODE, "")) # New
                if hasattr(self, 'electrolyte_var'): self.electrolyte_var.set(preset.get(CONFIG_ELECTROLYTE, "")) # New
                if hasattr(self, 'electrolyte_molarity_var'): self.electrolyte_molarity_var.set(preset.get(CONFIG_ELECTROLYTE_MOLARITY, "")) # New

                if hasattr(self, 'preset_name_var'):
                     self.preset_name_var.set(preset_name) # Update name field too

                self.add_status_message(f"Preset '{preset_name}' loaded.", STATUS_SUCCESS)

            else:
                 # This case should theoretically not be reachable if selection is from listbox
                 self.add_status_message(f"Preset '{preset_name}' data not found.", STATUS_ERROR)

        except Exception as e:
            self.add_status_message(f"Error loading preset '{preset_name}': {e}", STATUS_ERROR)
            traceback.print_exc()


    def delete_preset(self) -> None:
        """Deletes the selected preset."""
        # Check if listbox widget exists
        if not hasattr(self, 'preset_listbox') or not self.preset_listbox or not self.preset_listbox.winfo_exists():
             self.add_status_message("Cannot delete preset: Presets list not available.", STATUS_ERROR)
             return

        selection = self.preset_listbox.curselection()
        if not selection:
            messagebox.showinfo("Selection Required", "Please select a preset from the list to delete.")
            return
        if self.is_logging:
             messagebox.showwarning("Logging Active", "Cannot delete preset while logging is active.")
             return
         # Check if root window exists for messagebox
        if not self.root.winfo_exists():
             self.add_status_message("Cannot delete preset: Main window is closed.", STATUS_ERROR)
             return

        try:
            preset_name = self.preset_listbox.get(selection[0])
            if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the preset '{preset_name}'?"):
                if hasattr(self, 'config_manager') and self.config_manager and self.config_manager.delete_preset(preset_name):
                    self.load_presets_listbox() # Refresh listbox
                    # Clear preset name entry if the deleted preset's name was there
                    if hasattr(self, 'preset_name_var') and self.preset_name_var.get() == preset_name:
                         self.preset_name_var.set("")
                    self.add_status_message(f"Preset '{preset_name}' deleted.", STATUS_INFO)
                else:
                     # Should not happen if selected from listbox
                     self.add_status_message(f"Preset '{preset_name}' could not be deleted.", STATUS_WARNING)
        except Exception as e:
            self.add_status_message(f"Error deleting preset: {e}", STATUS_ERROR)
            traceback.print_exc()


    def add_status_message(self, message: str, message_type: str = STATUS_INFO) -> None:
        """Adds a timestamped message to the log tab and updates the status bar."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"

        # Add to log text widget
        try:
            # Check if the widget exists and is not being destroyed
            if hasattr(self, 'log_text') and self.log_text and self.log_text.winfo_exists():
                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, log_message, message_type)
                self.log_text.see(tk.END) # Auto-scroll
                self.log_text.config(state=tk.DISABLED)
            else:
                # If widget is gone, print to console as fallback
                 print(f"Log Widget Error: {log_message.strip()}")

        except tk.TclError:
            # Handle TclError explicitly if widget is in a bad state
            print(f"Log Error (TclError): {log_message.strip()}")
        except Exception as e:
            print(f"Log Error: {e}\nMessage: {log_message.strip()}")

        # Update status indicator bar (use simpler message, maybe truncated)
        status_text = message.split('\n')[0] # Show first line only
        try:
            # Check if status indicator widget exists
            if hasattr(self, 'status_indicator') and self.status_indicator and self.status_indicator.winfo_exists():
                if hasattr(self, 'status_var'):
                    self.status_var.set(status_text)
                # Set status indicator color based on type
                color = "black" # Default
                if message_type == STATUS_INFO: color = "blue"
                elif message_type == STATUS_SUCCESS: color = "green"
                elif message_type == STATUS_WARNING: color = "dark orange"
                elif message_type == STATUS_ERROR: color = "red"
                self.status_indicator.config(foreground=color)
        except tk.TclError:
             pass # Ignore if widget destroyed
        except Exception as e:
             print(f"Status Indicator Error: {e}") # Log other errors

    def clear_log(self) -> None:
        """Clears the log text widget."""
        try:
            # Check if log text widget exists
            if hasattr(self, 'log_text') and self.log_text and self.log_text.winfo_exists():
                self.log_text.config(state=tk.NORMAL)
                self.log_text.delete(1.0, tk.END)
                self.log_text.config(state=tk.DISABLED)
                self.add_status_message("Log cleared.", STATUS_INFO) # Add confirmation to log
            else:
                 print("Log widget not found to clear.")
        except tk.TclError:
            pass # Ignore if widget destroyed

    def _validate_float_input(self, value_str: str, field_name: str, allow_zero: bool = False, allow_negative: bool = False) -> float:
         """Validates string to float with optional constraints."""
         try:
             value = float(value_str)
             if not allow_negative and value < 0:
                  raise ValueError(f"{field_name} must be non-negative.")
             if not allow_zero and value == 0:
                  raise ValueError(f"{field_name} must be non-zero.")
             # Add max value checks if necessary (e.g., based on instrument limits)
             return value
         except ValueError:
              # Catch float conversion error and constraint violations
              raise ValueError(f"Invalid input for {field_name}. Please enter a valid number.")

    def _validate_int_input(self, value_str: str, field_name: str, min_val: int = 1) -> int:
         """Validates string to integer with optional minimum."""
         try:
             value = int(value_str)
             if value < min_val:
                  raise ValueError(f"{field_name} must be an integer of at least {min_val}.")
             return value
         except ValueError:
              raise ValueError(f"Invalid input for {field_name}. Please enter a valid integer.")


    def start_logging(self) -> None:
        """Validates settings, creates logger, connects, and starts logging."""
        # Check if root window exists
        if not self.root.winfo_exists():
             self.add_status_message("Cannot start logging: Main window is closed.", STATUS_ERROR)
             return


        if self.is_logging:
            messagebox.showwarning("Already Running", "Logging is already in progress.")
            return

        try:
            # --- Apply and Validate All Settings First ---
            # This will also save the config (including notes if they were saved manually)
            # Call save_notes explicitly here to ensure current notes are in config before logger reads it
            self.save_notes() # Ensure notes are saved to config before starting logger

            self.apply_and_save_config() # This runs validations internally and saves the config

            # --- Get current config after validation ---
            if hasattr(self, 'config_manager') and self.config_manager:
                 current_config = self.config_manager.config
            else:
                 self.add_status_message("Config manager not available. Cannot start.", STATUS_ERROR)
                 return

            is_simulating = current_config.get(CONFIG_SIMULATION_MODE, False) # Use .get with default

            # Resource name check only if not simulating
            if not is_simulating:
                resource_name = current_config.get(CONFIG_RESOURCE_NAME, "") # Use .get with default
                if not resource_name:
                    raise ValueError("VISA Resource Name cannot be empty when not in Simulation Mode.")
                if "Missing" in resource_name or "Error" in resource_name or "None" in resource_name:
                     raise ValueError("Invalid VISA Resource selected. Please scan and select a valid device.")


            # --- Create Logger Instance ---
            # Ensure DataManager is using the latest max_plot_points from config
            max_plot_points = current_config.get(CONFIG_MAX_PLOT_POINTS, 1000) # Use .get with default
            self.data_manager = DataManager(max_plot_points=max_plot_points)
            # Clear any old data points from the previous run before starting
            self.data_manager.clear_plot_data()

            self.logger = DataLogger(
                current_config, self.data_manager,
                self.status_queue, self.error_queue
            )

            # --- Connect/Start in Background ---
            self.add_status_message("Preparing to start logging...", STATUS_INFO)
            self.update_ui_state(True) # Disable UI immediately
            # Check if notebook exists before selecting tab
            if hasattr(self, 'notebook') and self.notebook and self.notebook.winfo_exists():
                 self.notebook.select(1) # Switch to plot tab
            self.executor.submit(self._connect_and_start_logging_task)

        except ValueError as e: # Catch validation errors from apply_and_save_config or specific checks here
            messagebox.showerror("Configuration Error", str(e))
            self.update_ui_state(False) # Re-enable UI if start fails early
        except Exception as e:
            messagebox.showerror("Start Error", f"An unexpected error occurred before starting: {e}")
            traceback.print_exc()
            self.update_ui_state(False) # Re-enable UI

    def _connect_and_start_logging_task(self) -> None:
        """Task to connect and start logging, run in the executor."""
        # Check if logger instance was successfully created
        if hasattr(self, 'logger') and self.logger is None:
             # Check if root window exists before putting error message
             if self.root.winfo_exists():
                 self.error_queue.put((STATUS_ERROR, "Logging instance not created. Cannot start."))
                 self.root.after(0, self._handle_start_failure)
             return
        elif not hasattr(self, 'logger'):
             if self.root.winfo_exists():
                 self.error_queue.put((STATUS_ERROR, "Logger attribute not initialized. Cannot start."))
                 self.root.after(0, self._handle_initialization_error, "Logger attribute missing during connection task.")
             return


        try:
            if self.logger and self.logger.connect(): # Handles simulation check internally
                if self.logger.start():
                    # Successfully started, update UI state (already done) and start animation
                    self.root.after(0, self._post_logging_start_tasks)
                else:
                    # Failed to start thread or instrument setup after connect
                    if self.root.winfo_exists():
                         self.error_queue.put((STATUS_ERROR, "Failed to start data logging process after connection."))
                         self.root.after(0, self._handle_start_failure) # Cleanup in main thread
            else:
                 # Connection failed (or PyVISA missing/simulation off)
                 # Error message should already be in the queue from connect()
                 if self.root.winfo_exists():
                     self.root.after(0, self._handle_start_failure) # Cleanup in main thread

        except Exception as e:
            # Catch errors within the task itself
            if self.root.winfo_exists():
                 self.error_queue.put((STATUS_ERROR, f"Critical error during connection/start task: {e}"))
                 traceback.print_exc()
                 self.root.after(0, self._handle_start_failure) # Cleanup in main thread
            else:
                 print(f"Critical error during connection/start task (root window closed): {e}")
                 traceback.print_exc()


    def _post_logging_start_tasks(self) -> None:
        """UI tasks after logging successfully starts (called via root.after)."""
        # Check if the application root still exists before performing UI operations
        if self.root.winfo_exists():
            self.add_status_message("Logging started successfully.", STATUS_SUCCESS)
            self.start_animation()
        else:
             print("Root window closed after logging started task completed.")


    def _handle_start_failure(self) -> None:
        """Handles UI cleanup if the start logging task fails."""
        # Check if the application root still exists before performing UI operations
        if self.root.winfo_exists():
            self.add_status_message("Logging failed to start.", STATUS_ERROR)
            # Attempt to gracefully stop/cleanup logger resources if partially started
            if hasattr(self, 'logger') and self.logger:
                # Pass empty notes here as logging didn't fully start
                self.logger.stop(notes_content="")
                self.logger = None
            self.update_ui_state(False) # Re-enable the UI
        else:
             print("Root window closed after start failure task completed.")


    def stop_logging(self) -> None:
        """Stops the data logging process."""
        # Check if root window exists before proceeding
        if not self.root.winfo_exists():
             return # Cannot stop if window is gone

        if not self.is_logging:
            # self.add_status_message("Logging is not currently active.", STATUS_INFO)
            return # Do nothing if already stopped

        self.add_status_message("Stopping logging...", STATUS_INFO)

        # Stop the animation first to prevent errors trying to plot stale data
        # Check if ani exists and has an event_source before stopping
        if hasattr(self, 'ani') and self.ani and hasattr(self.ani, 'event_source') and self.ani.event_source:
            try:
                self.ani.event_source.stop()
            except Exception as e:
                 self.add_status_message(f"Minor error stopping animation: {e}", STATUS_WARNING)
            self.ani = None

        # Get the current notes content from the GUI thread before stopping the logger thread
        notes_content = ""
        if hasattr(self, 'notes_text') and self.notes_text and self.notes_text.winfo_exists():
             try:
                 notes_content = self.notes_text.get("1.0", tk.END).strip()
             except Exception as e:
                  self.add_status_message(f"Error getting notes content for export: {e}", STATUS_WARNING)


        # Signal the logger thread to stop (turns off instrument, closes file/connection)
        if hasattr(self, 'logger') and self.logger:
            # Pass the notes content to the logger's stop method
            self.logger.stop(notes_content=notes_content) # Pass notes
            self.logger = None # Dereference

        # Clear plot data visually after stopping animation/logger
        # Check if data_manager exists and root window exists
        if hasattr(self, 'data_manager') and self.data_manager and self.root.winfo_exists():
             self.data_manager.clear_plot_data()
             if hasattr(self, 'fig') and self.fig and hasattr(self, 'canvas') and self.canvas and self.canvas.get_tk_widget().winfo_exists(): # Redraw empty plot axes
                try:
                     self._update_plot_axes() # Update limits to default
                     self.canvas.draw_idle()
                except Exception as e:
                     print(f"Error redrawing plot on stop: {e}")


        # Update UI state LAST
        if self.root.winfo_exists():
            self.update_ui_state(False)
            # Final status message is added by the logger's stop method after export


    def start_animation(self) -> None:
            """Starts the Matplotlib FuncAnimation for real-time plotting."""
            # Check if root window exists before starting animation
            if not self.root.winfo_exists():
                 return

            # Check if ani exists and is valid BEFORE checking is_running
            if hasattr(self, 'ani') and self.ani:
                if hasattr(self.ani, 'event_source') and self.ani.event_source and self.ani.event_source.is_running():
                     self.add_status_message("Animation already running.", STATUS_WARNING)
                     return # Avoid multiple animations
                else:
                     # Existing ani object is not running or invalid, allow creating a new one
                     self.ani = None # Clear the invalid reference

            if not hasattr(self, 'fig') or not self.fig or not hasattr(self, 'canvas') or not self.canvas or not self.canvas.get_tk_widget().winfo_exists():
                self.add_status_message("Plot figure/canvas not ready for animation.", STATUS_ERROR)
                return

            try:
                # Clear any residual plot data from previous runs - already done in start_logging
                # self.data_manager.clear_plot_data()

                # Interval from config
                interval_ms = self.config_manager.config.get(CONFIG_UPDATE_INTERVAL, 200) # Use .get with default
                if interval_ms < MIN_UPDATE_INTERVAL_MS: 
                     interval_ms = MIN_UPDATE_INTERVAL_MS 

                self.ani = animation.FuncAnimation(
                    self.fig,
                    self.update_plot,          # Function to call
                    interval=interval_ms,      
                    blit=False,                 # Disable blitting to fix flashing with dynamic axes
                    cache_frame_data=False,    # Avoid memory leak potential
                    repeat=True                # Keep running
                )
                self.canvas.draw_idle() # Initial draw call
                self.add_status_message(f"Plot animation started (Interval: {interval_ms}ms, Blit: False).", STATUS_INFO) # Update status message

            except Exception as e:
                self.add_status_message(f"Error starting plot animation: {e}", STATUS_ERROR)
                traceback.print_exc()
                self.ani = None # Ensure ani is None if start fails

    def update_plot(self, frame: int) -> Tuple[Line2D, Line2D, Line2D, Line2D]:
        """
        Updates the plot with ONE new data point from the queue.
        Called by FuncAnimation.

        Args:
            frame: Animation frame number (unused).

        Returns:
            Tuple: Tuple of updated Line2D artists for blitting.
        """
        # Ensure artists exist
        artists = []
        if hasattr(self, 'line_v') and self.line_v: artists.append(self.line_v)
        if hasattr(self, 'line_i') and self.line_i: artists.append(self.line_i)
        if hasattr(self, 'line_p') and self.line_p: artists.append(self.line_p)
        if hasattr(self, 'line_r') and self.line_r: artists.append(self.line_r)

        if len(artists) != 4 or not self.is_logging or not hasattr(self, 'data_manager') or not self.data_manager or not hasattr(self, 'canvas') or not self.canvas or not self.canvas.get_tk_widget().winfo_exists():
             return tuple(artists) # type: ignore - Return what we have


        data_processed = False
        try:
            if hasattr(self, 'data_manager') and self.data_manager:
                 while not self.data_manager.empty():
                      try:
                         data_point = self.data_manager.get_nowait() # [t, v, c, p, r]
                         self.data_manager.append_for_plotting(*data_point)
                         data_processed = True
                         break 
                      except queue.Empty:
                           pass 

        except Exception as e:
            if "invalid command name" not in str(e): 
                self.add_status_message(f"Plot Error (Queue): {e}", STATUS_ERROR)
                traceback.print_exc() 

        if data_processed and hasattr(self, 'data_manager') and self.data_manager and hasattr(self.data_manager, 'time_data') and len(self.data_manager.time_data) > 0:
             try:
                time_arr = np.array(self.data_manager.time_data)
                voltage_arr = np.array(self.data_manager.voltage_data)
                current_arr = np.array(self.data_manager.current_data)
                power_arr = np.array(self.data_manager.power_data)
                resistance_arr = np.array(self.data_manager.resistance_data)

                if hasattr(self, 'line_v') and self.line_v: self.line_v.set_data(time_arr, voltage_arr)
                if hasattr(self, 'line_i') and self.line_i: self.line_i.set_data(time_arr, current_arr)
                if hasattr(self, 'line_p') and self.line_p: self.line_p.set_data(time_arr, power_arr)
                if hasattr(self, 'line_r') and self.line_r: self.line_r.set_data(time_arr, resistance_arr)


                if hasattr(self, 'canvas') and self.canvas and self.canvas.get_tk_widget().winfo_exists():
                    self._update_plot_axes()


             except Exception as e:
                  if "invalid command name" not in str(e): 
                      self.add_status_message(f"Plot Error (Update): {e}", STATUS_ERROR)
                      traceback.print_exc() 

        return tuple(artists) # type: ignore


    def _update_plot_axes(self, reset_limits: bool = False) -> None:
        """
        Auto-scales plot axes based on current data in deques, ensuring origin at 0,0.
        Optionally resets limits to default.
        """
        if not hasattr(self, 'fig') or not self.fig or \
           not hasattr(self, 'ax_v') or not self.ax_v or \
           not hasattr(self, 'ax_i') or not self.ax_i or \
           not hasattr(self, 'ax_p') or not self.ax_p or \
           not hasattr(self, 'ax_r') or not self.ax_r:
            print("Warning: Plot figure or axes not available to update axes.")
            return

        if not hasattr(self, 'data_manager') or not self.data_manager:
             print("Warning: Data manager not available to update plot axes.")
             return 

        time_data = self.data_manager.time_data
        voltage_data = self.data_manager.voltage_data
        current_data = self.data_manager.current_data
        power_data = self.data_manager.power_data
        resistance_data = self.data_manager.resistance_data

        axes_list = [self.ax_v, self.ax_i, self.ax_p, self.ax_r]
        data_lists = [voltage_data, current_data, power_data, resistance_data]

        t_min = 0.0
        if time_data:
            t_max = time_data[-1]
            if t_max == t_min: 
                t_max += 0.5
            t_range = t_max - t_min
            padding_right = max(0.1 * t_range, 0.1) if t_range > 0 else 0.1
            if hasattr(self, 'ax_r') and self.ax_r:
                self.ax_r.set_xlim(t_min, t_max + padding_right)
        else:
            # Default for empty plot, x-axis from 0 to 10
            if hasattr(self, 'ax_r') and self.ax_r:
                self.ax_r.set_xlim(0, 10)

        # --- Y-Axis Scaling (Individual) ---
        for ax, data in zip(axes_list, data_lists):
            # Check if axis exists and data is not empty
            if not ax:
                continue # Skip to next axis if axis object is None

            if data:
                # Special handling for resistance 'inf'
                if ax == self.ax_r:
                    finite_data = [r for r in data if np.isfinite(r)]
                    if not finite_data: # All inf or empty
                         y_min, y_max = 0, 100 # Default range if no finite resistance, start from 0
                    else:
                        y_min, y_max = min(finite_data), max(finite_data)
                        # Force y_min to 0 if the minimum finite data is greater than 0
                        y_min = max(0.0, y_min)
                else:
                    y_min, y_max = min(data), max(data)
                    # Force y_min to 0 if the minimum data is greater than 0
                    y_min = max(0.0, y_min)

                # Calculate padding
                y_range = y_max - y_min
                if y_range == 0: # Handle constant data
                    padding = abs(y_min * 0.1) if y_min != 0 else 0.5 # 10% or fixed padding
                else:
                    padding = 0.05 * y_range # 5% padding

                # Ensure padding is not excessively small, but also not huge if data is near zero
                min_padding = 1e-9 # Very small padding
                if abs(y_max) < 1 and abs(y_min) < 1: # If data is small, use a fixed small padding
                     padding = max(padding, 0.01) # Ensure at least 0.01 padding for small values
                else:
                    padding = max(padding, abs(y_max) * 0.001, abs(y_min) * 0.001, min_padding) # Ensure padding relative to value size or min_padding

                # Prevent inverted limits if data range is tiny or negative (unlikely but safeguard)
                # Lower limit is already forced to 0 or max(0, min_data)
                lower_limit = y_min
                upper_limit = y_max + padding
                if lower_limit >= upper_limit:
                     # If limits are inverted or equal, create a small valid range
                     center = (y_min + y_max) / 2.0 if y_range == 0 else y_min # Use min if range is 0
                     lower_limit = center - 0.5
                     upper_limit = center + 0.5
                     if lower_limit == upper_limit: upper_limit += 1.0 # Ensure range is not zero

                 # Ensure the lower limit is not less than 0 after padding adjustments, especially for voltage, current, power
                if ax in [self.ax_v, self.ax_i, self.ax_p]: # Apply 0 lower bound constraint
                    ax.set_ylim(max(0.0, lower_limit), upper_limit)
                else: # Resistance can potentially be negative (though unlikely in this application), apply 0 constraint if data is positive, otherwise allow negative
                     ax.set_ylim(max(0.0, lower_limit) if all(r >= 0 for r in data) else lower_limit, upper_limit)


            else: # Data is empty for this axis
                # Set default Y limits for empty data, starting from 0
                if ax == self.ax_v: ax.set_ylim(0, 1)
                elif ax == self.ax_i: ax.set_ylim(0, 0.1) # Smaller range for current default
                elif ax == self.ax_p: ax.set_ylim(0, 1)
                elif ax == self.ax_r: ax.set_ylim(0, 100)


        # Explicitly tell the canvas to redraw after updating axes
        # This might be necessary when blitting is enabled to ensure the background updates
        if hasattr(self, 'canvas') and self.canvas and self.canvas.get_tk_widget().winfo_exists():
             self.canvas.draw_idle()


    def check_queues(self) -> None:
        """Periodically check status/error queues from logger and update GUI."""
        # Check if the application root still exists before proceeding
        if not self.root.winfo_exists():
            return # Stop scheduling if window is gone

        try:
            # Check status queue
            if hasattr(self, 'status_queue') and self.status_queue:
                 while not self.status_queue.empty():
                      try:
                          message_type, message = self.status_queue.get_nowait()
                          self.add_status_message(message, message_type)
                      except queue.Empty:
                           break # Exit loop if queue is suddenly empty

            # Check error queue
            if hasattr(self, 'error_queue') and self.error_queue:
                 while not self.error_queue.empty():
                      try:
                          message_type, message = self.error_queue.get_nowait()
                          self.add_status_message(message, message_type)
                      except queue.Empty:
                           break # Exit loop if queue is suddenly empty


        except Exception as e:
            # Prevent queue checking errors from crashing the app
            print(f"Error checking queues: {e}")
            traceback.print_exc()

        # Schedule next check
        # Store the ID returned by after
        if self.root.winfo_exists(): # Check again before scheduling
             self._after_check_queues_id = self.root.after(100, self.check_queues) # Check every 100ms

    def on_closing(self) -> None:
        """Handles window closing: stops logging, saves config, cleans up."""
        # Check if root window exists before proceeding
        if not self.root.winfo_exists():
             return

        # Cancel the scheduled check_queues call if it exists and root is valid
        # Add checks to prevent errors if _after_check_queues_id is None or invalid
        if hasattr(self, '_after_check_queues_id') and self._after_check_queues_id is not None and self.root.winfo_exists():
            try:
                # Check if the ID is still valid before cancelling
                # This check is not directly available in Tkinter, but wrapping in try/except is the standard way
                self.root.after_cancel(self._after_check_queues_id)
                self._after_check_queues_id = None # Clear the ID after cancelling
            except tk.TclError:
                # Handle cases where after_cancel fails (e.g., ID already invalid or window destroyed)
                # This is the expected error if the 'after' command is already gone
                print("Info: after_cancel failed (TclError), likely already cancelled or window destroyed.")
                self._after_check_queues_id = None # Ensure ID is cleared even on TclError
            except Exception as e:
                 print(f"Error cancelling check_queues after: {e}") # Log other errors
                 self._after_check_queues_id = None # Ensure ID is cleared even on other errors


        if self.is_logging:
            # Check if messagebox can be shown
            if self.root.winfo_exists():
                 if messagebox.askyesno("Confirm Exit", "Logging is in progress. Stop logging and exit?"):
                     self.stop_logging()
                     # Give stop_logging a moment to cleanup (stop() already has a join timeout)
                     # It's better to wait for the logger thread to finish its cleanup using join,
                     # but stop() already does this with a timeout. A small additional sleep
                     # might help ensure final messages propagate before destruction.
                     self.root.update_idletasks() # Process any pending events
                     # time.sleep(0.1) # Small grace period (optional, stop() join might be sufficient)
                 else:
                     return # Don't close
            else:
                 # If messagebox couldn't be shown, proceed with stopping
                 print("Root window closed, stopping logging automatically.")
                 self.stop_logging()
                 # time.sleep(0.1) # Small grace period


        # Save current settings and notes from UI before exiting
        try:
            # Only save if the root window still exists (not destroyed by stop_logging)
            if self.root.winfo_exists():
                 self.save_notes() # Save notes first
                 self.apply_and_save_config() # Saves config, which includes notes
        except Exception as e:
             print(f"Warning: Could not save config on exit: {e}") # Non-critical

        # Clean up animation if somehow still running
        # Check if ani exists and has an event_source before stopping
        if hasattr(self, 'ani') and self.ani and hasattr(self.ani, 'event_source') and self.ani.event_source:
            try:
                self.ani.event_source.stop()
            except Exception: pass
            self.ani = None

        # Close matplotlib figures
        try:
            plt.close('all')
        except Exception: pass # Ignore errors if figures are already closed

        # Shutdown thread pool executor gracefully
        # wait=False prevents hanging if threads are stuck
        if hasattr(self, 'executor') and self.executor:
            self.executor.shutdown(wait=False, cancel_futures=True) # Also try to cancel running tasks


        # Destroy the main window if it still exists
        if self.root.winfo_exists():
            self.root.destroy()


# --- Main Execution ---
if __name__ == "__main__":
    # --- Exception Handling Hook ---
    def show_error(exc_type, exc_value, exc_traceback):
        """Global exception handler: Shows error in messagebox or prints."""
        error_msg = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        print(f"Unhandled Exception:\n{error_msg}") # Always print to console

        # Try showing in messagebox if Tkinter is usable
        try:
            # Check if a root window exists and isn't being destroyed
            # Access tk._default_root carefully as it might not be set or might be None
            # if Tkinter initialization failed or root was destroyed.
            is_tkinter_ready = False
            if hasattr(tk, '_default_root') and tk._default_root is not None:
                 if tk._default_root.winfo_exists():
                      is_tkinter_ready = True

            if is_tkinter_ready:
                messagebox.showerror("Unhandled Exception", "An unhandled exception occurred:\n\n" + error_msg)
            else:
                print("Could not show exception in messagebox (Tkinter not ready).")

        except tk.TclError:
            print("Could not show exception in messagebox (Tkinter TclError).")
        except Exception as e:
            print(f"Could not show exception in messagebox: {e}")

    sys.excepthook = show_error

    # --- Initialize Root Window ---
    # Put in a try-except block in case ThemedTk initialization fails critically
    root = None
    try:
        root = ThemedTk()
        # Theme is set *after* config loaded in PowerLoggerApp.__init__

        # --- Create and Run Application ---
        # Wrap app creation in try-except as well
        app = None
        try:
             app = PowerLoggerApp(root)
        except Exception:
            # Error should be handled by _handle_initialization_error
            pass # Avoid double handling


        if app and root and root.winfo_exists():
             root.mainloop()

    except Exception:
        # Catch critical errors during root window initialization or mainloop
        # show_error hook should handle displaying/logging it
        # If root was created, ensure it's destroyed to clean up Tkinter resources
        if root and root.winfo_exists():
             try:
                 root.destroy()
             except Exception: pass # Ignore errors during final destruction
        sys.exit(1) # Exit
