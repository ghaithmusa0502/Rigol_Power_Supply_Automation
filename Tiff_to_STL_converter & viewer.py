# Standard library imports
import sys
import os

# PyQt5 imports for GUI components
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget,
    QFileDialog, QLabel, QLineEdit, QCheckBox, QMessageBox, QFrame,
    QHBoxLayout, QSizePolicy
)
from PyQt5.QtGui import QPixmap, QIcon
from PyQt5.QtCore import Qt, QSize

# External libraries for image processing and mesh generation
from PIL import Image
import numpy as np
from stl import mesh  # For saving STL files

# PyVista for 3D rendering and mesh manipulation
import pyvista as pv
from pyvistaqt import QtInteractor  # Qt integration for PyVista

# Main application class for converting images to STL files and viewing them
class ImageToSTLApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.image_path = None  # Path to the loaded image
        self.stl_mesh_data = None  # Data structure for storing STL mesh
        self.pyvista_mesh = None  # PyVista mesh object

        self.viewer_widget = None  # PyVista 3D viewer
        self.plotter = None  # PyVista plotter object

        self.init_ui()  # Initialize GUI

    def init_ui(self):
        # Basic window settings
        self.setWindowTitle('Image to STL Converter & Viewer')
        self.setGeometry(100, 100, 1200, 900)
        self.setWindowIcon(QIcon(self.get_resource_path('icon.png')))

        # Set up the main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_h_layout = QHBoxLayout()
        central_widget.setLayout(main_h_layout)

        # Sidebar for controls
        controls_v_layout = QVBoxLayout()
        controls_widget = QWidget()
        controls_widget.setLayout(controls_v_layout)
        controls_widget.setFixedWidth(400)
        main_h_layout.addWidget(controls_widget)

        # Section: Load image or STL
        image_section_frame = QFrame()
        image_section_frame.setFrameShape(QFrame.StyledPanel)
        image_layout = QVBoxLayout()
        image_section_frame.setLayout(image_layout)
        controls_v_layout.addWidget(image_section_frame)

        image_layout.addWidget(QLabel("<h2>1. Load Image or STL</h2>"))
        self.load_stl_btn = QPushButton('Load Existing STL...')
        self.load_stl_btn.clicked.connect(self.load_stl)
        image_layout.addWidget(self.load_stl_btn)

        self.load_image_btn = QPushButton('Browse Image for Conversion...')
        self.load_image_btn.clicked.connect(self.load_image)
        image_layout.addWidget(self.load_image_btn)

        self.image_display_label = QLabel('No image loaded.')
        self.image_display_label.setAlignment(Qt.AlignCenter)
        self.image_display_label.setFixedSize(250, 250)
        self.image_display_label.setScaledContents(True)
        image_layout.addWidget(self.image_display_label, alignment=Qt.AlignCenter)

        # Section: Settings
        settings_section_frame = QFrame()
        settings_section_frame.setFrameShape(QFrame.StyledPanel)
        settings_layout = QVBoxLayout()
        settings_section_frame.setLayout(settings_layout)
        controls_v_layout.addWidget(settings_section_frame)

        settings_layout.addWidget(QLabel("<h2>2. Adjust Conversion Settings</h2>"))

        # Max height setting
        max_height_layout = QHBoxLayout()
        max_height_layout.addWidget(QLabel("Max Height (mm):"))
        self.max_height_input = QLineEdit('10.0')
        self.max_height_input.setToolTip("Sets the maximum height of the 3D model.")
        max_height_layout.addWidget(self.max_height_input)
        settings_layout.addLayout(max_height_layout)

        # Scale setting
        scale_layout = QHBoxLayout()
        scale_layout.addWidget(QLabel("Pixels per Unit (mm):"))
        self.scale_input = QLineEdit('0.1')
        self.scale_input.setToolTip("Determines the size of each pixel in millimeters (e.g., 0.1 means 10 pixels = 1mm).")
        scale_layout.addWidget(self.scale_input)
        settings_layout.addLayout(scale_layout)

        # Invert height checkbox
        self.invert_height_checkbox = QCheckBox('Invert Height (darker pixels = taller)')
        self.invert_height_checkbox.setToolTip("Check this to make darker areas of the image appear taller in the 3D model.")
        settings_layout.addWidget(self.invert_height_checkbox)

        # Section: Convert and Save
        actions_section_frame = QFrame()
        actions_section_frame.setFrameShape(QFrame.StyledPanel)
        actions_layout = QVBoxLayout()
        actions_section_frame.setLayout(actions_layout)
        controls_v_layout.addWidget(actions_section_frame)

        actions_layout.addWidget(QLabel("<h2>3. Convert & Save</h2>"))

        self.convert_btn = QPushButton('Convert Image to STL')
        self.convert_btn.clicked.connect(self.convert_image_to_stl)
        self.convert_btn.setEnabled(False)
        actions_layout.addWidget(self.convert_btn)

        self.save_stl_btn = QPushButton('Save Current STL File...')
        self.save_stl_btn.clicked.connect(self.save_stl)
        self.save_stl_btn.setEnabled(False)
        actions_layout.addWidget(self.save_stl_btn)

        controls_v_layout.addStretch()  # Push controls up

        # 3D viewer widget setup
        self.viewer_widget = QtInteractor(self)
        self.plotter = self.viewer_widget.interactor
        self.plotter.set_background('white')
        self.plotter.show_axes()
        self.plotter.show_grid()
        main_h_layout.addWidget(self.viewer_widget)

        # Status bar
        self.status_label = QLabel('Ready: Load an image or an STL file.')
        self.statusBar().addWidget(self.status_label)

    # Get path to resources (useful for PyInstaller)
    def get_resource_path(self, relative_path):
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)

    # Load an image and display it
    def load_image(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self, 'Open Image', '',
            'Image Files (*.png *.jpg *.jpeg *.bmp *.gif *.tif *.tiff)'
        )
        if file_path:
            self.image_path = file_path
            self.status_label.setText(f'Loaded Image: {os.path.basename(file_path)}')
            pixmap = QPixmap(file_path)
            scaled_pixmap = pixmap.scaled(self.image_display_label.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.image_display_label.setPixmap(scaled_pixmap)

            self.convert_btn.setEnabled(True)
            self.save_stl_btn.setEnabled(False)
            self.stl_mesh_data = None
            self.pyvista_mesh = None
            self.plotter.clear()
            self.plotter.reset_camera()

    # Load and visualize an existing STL file
    def load_stl(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(self, 'Open STL File', '', 'STL Files (*.stl)')
        if file_path:
            try:
                self.pyvista_mesh = pv.read(file_path)

                # Ensure the mesh is valid
                if not self.pyvista_mesh.faces.any():
                    raise ValueError("Loaded STL has no faces.")
                if self.pyvista_mesh.faces.shape[0] % 4 != 0:
                    raise ValueError("Loaded STL faces are not in expected PyVista triangular format.")

                vertices = self.pyvista_mesh.points
                faces = self.pyvista_mesh.faces.reshape(-1, 4)[:, 1:] if self.pyvista_mesh.n_cells > 0 else np.array([])

                # Create STL mesh data if faces exist
                if faces.size > 0:
                    self.stl_mesh_data = mesh.Mesh(np.zeros(faces.shape[0], dtype=mesh.Mesh.dtype))
                    for i, f in enumerate(faces):
                        if not (np.all(f >= 0) and np.all(f < len(vertices))):
                            raise ValueError(f"Face indices {f} out of bounds.")
                        self.stl_mesh_data.vectors[i] = vertices[f]

                # Generate color gradient for better 3D visualization
                if vertices.size > 0:
                    z_values = vertices[:, 2]
                    min_z, max_z = np.min(z_values), np.max(z_values)
                    BLACK, WHITE, GOLD = np.array([0.0]*3), np.array([1.0]*3), np.array([1.0, 0.843, 0.0])
                    mid_z = (min_z + max_z) / 2
                    colors = []
                    for height in z_values:
                        if height < mid_z:
                            ratio = (height - min_z) / (mid_z - min_z) if mid_z != min_z else 0
                            color = BLACK * (1 - ratio) + GOLD * ratio
                        else:
                            ratio = (height - mid_z) / (max_z - mid_z) if max_z != mid_z else 0
                            color = GOLD * (1 - ratio) + WHITE * ratio
                        colors.append(color)
                    self.pyvista_mesh['colors'] = np.array(colors)

                # Display STL in 3D viewer
                self.plotter.clear()
                self.plotter.add_mesh(self.pyvista_mesh, scalars='colors', rgb=True, show_edges=False)
                self.plotter.reset_camera()
                self.plotter.show_grid()

                self.status_label.setText(f'Loaded STL: {os.path.basename(file_path)}')
                self.convert_btn.setEnabled(False)
                self.save_stl_btn.setEnabled(self.stl_mesh_data is not None)

            # Handle loading errors
            except ValueError as ve:
                QMessageBox.critical(self, "Load STL Error", f"Failed to process STL mesh data. Details: {ve}")
                self.status_label.setText(f'Error loading STL: {ve}')
                self._reset_viewer_state()

            except Exception as e:
                QMessageBox.critical(self, "Load STL Error", f"An error occurred while loading the STL file: {e}")
                self.status_label.setText(f'Error loading STL: {e}')
                self._reset_viewer_state()

    # Helper to clear viewer state on error
    def _reset_viewer_state(self):
        self.pyvista_mesh = None
        self.stl_mesh_data = None
        self.plotter.clear()
        self.plotter.reset_camera()
        self.save_stl_btn.setEnabled(False)

    # Convert loaded image into STL mesh (implementation incomplete in snippet)
    def convert_image_to_stl(self):
        if not self.image_path:
            QMessageBox.warning(self, "No Image", "Please load an image first.")
            return

        try:
            raw_height_text = self.max_height_input.text()
            raw_scale_text = self.scale_input.text()

            print(f"DEBUG: Raw Max Height Text: '{raw_height_text}' (Length: {len(raw_height_text)})")
            print(f"DEBUG: Raw Scale Text: '{raw_scale_text}' (Length: {len(raw_scale_text)})")

            max_height_str = raw_height_text.strip()
            unit_per_pixel_str = raw_scale_text.strip()

            print(f"DEBUG: Stripped Max Height Text: '{max_height_str}' (Length: {len(max_height_str)})")
            print(f"DEBUG: Stripped Scale Text: '{unit_per_pixel_str}'...")
