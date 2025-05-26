# Rigol Power Supply Automation and logging script

This repository contains Python scripts designed to be used for manufacturing tip etching and examining TIFF STM outputs 

## Scripts

### 1. `Power Supply Stopper.py`

* **Purpose:** This script provides a comprehensive Tkinter-based GUI for controlling a programmable power supply. It offers more advanced features, including preset management, configurable export formats, detailed logging, and a more interactive user experience.
* **Key Features:**
    * **GUI Interface:** User-friendly interface built with Tkinter and ttkthemes.
    * **VISA Resource Scanning:** Automatically scans for and lists available VISA instruments.
    * **Configurable Parameters:** Allows setting of voltage, current, and stop threshold; supports above/below stop condition.
    * **Real-time Plotting:** Live plots for Voltage, Current, Power, and Resistance. Plot style is configurable.
    * **Data Logging & Export:**
        * Logs all data points in memory during the session.
        * Exports data to CSV, XLSX (Excel), or JSON formats.
        * Includes configuration settings and user notes in exported files.
    * **Preset Management:** Save and load configurations for different experimental setups.
    * **Simulation Mode:** Allows running the application without a physical instrument for testing or demonstration.
    * **Notes Section:** Add and save notes related to the experiment, which are included in data exports.
    * **Electrochemical Cell Information:** Fields to input Anode, Cathode, Electrolyte, and Molarity details, which are saved with the data.
    * **Status & Log Tabs:** Provides real-time status updates and a detailed event log.
    * **Customizable Appearance:** Selectable GUI themes and plot styles.
* **Usage:**
    1.  Ensure all dependencies are installed.
    2.  Run the script: `python "Power Supply Stopper.py"`
    3.  **Control Tab:**
        * Scan for VISA resources.
        * Set the desired Voltage, Current, and Stop Threshold.
        * Choose the save location for exported data.
        * Select the export format (CSV, XLSX, JSON, or All).
        * Optionally, fill in electrochemical cell information.
    4.  **Settings Tab:**
        * Configure plot update interval, max plot points, GUI theme, and plot style.
        * Enable/disable simulation mode.
        * Set the stop condition (current below or above threshold).
        * Manage presets (save, load, delete).
    5.  **Notes Tab:** Add any relevant experimental notes. These will be saved with the configuration and included in data exports.
    6.  Click "Start Logging". The application will switch to the "Plots" tab.
    7.  Click "Stop Logging" to end the experiment. Data will be exported according to the selected format and settings.
    8.  The "Log" tab shows a history of operations and events.
* **Dependencies:**
    * `tkinter` (usually part of Python's standard library)
    * `pyvisa` (for instrument communication)
    * `pandas` (for XLSX and JSON export)
    * `openpyxl` (for XLSX export, used by pandas)
    * `numpy` (for numerical operations, especially in plotting)
    * `matplotlib` (for plotting)
    * `ttkthemes` (optional, for enhanced GUI styling; falls back to default Tkinter styles if not found)
    * `Pillow` (PIL) (optional, for using `.ico` window icons; falls back if not found)
    * `winsound` (optional, for beep sound on Windows when threshold is met; script runs on other OS without it)
    * `psutil` System monitoring (CPU, battery, process stats) for optional integration
    * `zeroconf`  Future-ready support for auto-discovery of networked instruments and devices
    * `pyvisa-py`  Backend for pyvisa (pure Python implementation, no NI-VISA needed).
```bash
pip install tkinter pyvisa pandas openpyxl numpy matplotlib ttkthemes Pillow winsound psutil zeroconf pyvisa-py
```

Having only had access to one power supply, I can only assure success with the RIGOL DP 811A power suppluy

### Screenshots of Programs

### Power Supply Control Tab 1
![Screenshot of the Power Supply Control Tab](images/power_supply_control_tab.png)

### Power Supply Control Tab 2
![Screenshot of the Power Supply Control Tab](images/power_supply_control_tab_two.png)

### Real-time Plotting
![Screenshot showing real-time voltage and current plots](images/power_supply_plots.png)

### Image to STL Conversion
![Screenshot of the Image to STL converter with image loaded](images/stl_converter_main.png)

### 3D Model Viewer
![Screenshot showing a 3D model in the PyVista viewer](images/stl_viewer_model.png)


#  Limitations
   * It must be noted that when using the power supply controller, decreasing the logging time, down to 10ms, this will increase the plotting time, but will hold no bearing on the logging on exporting the file in JSON, CSV or XlSX.

# Notes
Chances are, if you are using these programs, it’s because you are doing the electrochemical etching project, so if you're doing this project after me (after 2024/25).

The parameters I had the most success with Nickel were the Constant voltage- 0.5 mols of HCL, 4V, 0.5 amps with a threshold of 0.09 amps with this parameter i got like sub-100nm radius of curvature.

For tungsten, I had less time but... Constant voltage- 2 mols of NaOH, 9V, 1 amp with a threshold of 0.032 amps, I got like 100nm average radius of curvature.

If I were you, and if I had more time, I’d experiment with the Constant current setting. From my brief experiments, it seemed to have potential and produce smoother tips.

Lowk this was more a coding project than a lab project, considering how much time I spent doing this instead of being in a Lab XD! And all this work to make it look good for a person I don't know XD! 

Anyway, other than that, there’s nothing else I can give you! Anyway, good luck! I hope you enjoy this project! And book as many sessions as possible. You gonna need them.

Final note, don’t get better tips than me!




