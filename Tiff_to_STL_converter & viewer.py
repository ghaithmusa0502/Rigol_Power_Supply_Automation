import sys
import os
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget,
    QFileDialog, QLabel, QLineEdit, QCheckBox, QMessageBox, QFrame,
    QHBoxLayout, QSizePolicy
)
from PyQt5.QtGui import QPixmap, QIcon
from PyQt5.QtCore import Qt, QSize
from PIL import Image
import numpy as np
from stl import mesh

import pyvista as pv
from pyvistaqt import QtInteractor

class ImageToSTLApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.image_path = None
        self.stl_mesh_data = None
        self.pyvista_mesh = None

        self.viewer_widget = None
        self.plotter = None

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle('Image to STL Converter & Viewer')
        self.setGeometry(100, 100, 1200, 900)
        self.setWindowIcon(QIcon(self.get_resource_path('icon.png')))

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_h_layout = QHBoxLayout()
        central_widget.setLayout(main_h_layout)

        controls_v_layout = QVBoxLayout()
        controls_widget = QWidget()
        controls_widget.setLayout(controls_v_layout)
        controls_widget.setFixedWidth(400)
        main_h_layout.addWidget(controls_widget)

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

        settings_section_frame = QFrame()
        settings_section_frame.setFrameShape(QFrame.StyledPanel)
        settings_layout = QVBoxLayout()
        settings_section_frame.setLayout(settings_layout)
        controls_v_layout.addWidget(settings_section_frame)

        settings_layout.addWidget(QLabel("<h2>2. Adjust Conversion Settings</h2>"))

        max_height_layout = QHBoxLayout()
        max_height_layout.addWidget(QLabel("Max Height (mm):"))
        self.max_height_input = QLineEdit('10.0')
        self.max_height_input.setToolTip("Sets the maximum height of the 3D model.")
        max_height_layout.addWidget(self.max_height_input)
        settings_layout.addLayout(max_height_layout)

        scale_layout = QHBoxLayout()
        scale_layout.addWidget(QLabel("Pixels per Unit (mm):"))
        self.scale_input = QLineEdit('0.1')
        self.scale_input.setToolTip("Determines the size of each pixel in millimeters (e.g., 0.1 means 10 pixels = 1mm).")
        scale_layout.addWidget(self.scale_input)
        settings_layout.addLayout(scale_layout)

        self.invert_height_checkbox = QCheckBox('Invert Height (darker pixels = taller)')
        self.invert_height_checkbox.setToolTip("Check this to make darker areas of the image appear taller in the 3D model.")
        settings_layout.addWidget(self.invert_height_checkbox)

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

        controls_v_layout.addStretch()

        self.viewer_widget = QtInteractor(self)
        self.plotter = self.viewer_widget.interactor
        self.plotter.set_background('white')
        self.plotter.show_axes()
        self.plotter.show_grid() # --- MODIFIED: line_width removed ---
        main_h_layout.addWidget(self.viewer_widget)

        self.status_label = QLabel('Ready: Load an image or an STL file.')
        self.statusBar().addWidget(self.status_label)

    def get_resource_path(self, relative_path):
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)

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

    def load_stl(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self, 'Open STL File', '',
            'STL Files (*.stl)'
        )
        if file_path:
            try:
                self.pyvista_mesh = pv.read(file_path)

                if not self.pyvista_mesh.faces.any():
                    raise ValueError("Loaded STL has no faces.")
                if self.pyvista_mesh.faces.shape[0] % 4 != 0:
                    raise ValueError("Loaded STL faces are not in expected PyVista triangular format.")

                vertices = self.pyvista_mesh.points

                if self.pyvista_mesh.n_cells > 0:
                    faces = self.pyvista_mesh.faces.reshape(-1, 4)[:, 1:]
                else:
                    faces = np.array([])

                if faces.size > 0:
                    self.stl_mesh_data = mesh.Mesh(np.zeros(faces.shape[0], dtype=mesh.Mesh.dtype))
                    for i, f in enumerate(faces):
                        if not (np.all(f >= 0) and np.all(f < len(vertices))):
                            raise ValueError(f"Face indices {f} out of bounds for vertices array of size {len(vertices)}.")
                        self.stl_mesh_data.vectors[i][0] = vertices[f[0]]
                        self.stl_mesh_data.vectors[i][1] = vertices[f[1]]
                        self.stl_mesh_data.vectors[i][2] = vertices[f[2]]
                else:
                    self.stl_mesh_data = None

                # --- COLORING CODE ---
                if vertices.size > 0:
                    z_values = vertices[:, 2]
                    min_z = np.min(z_values)
                    max_z = np.max(z_values)

                    BLACK = np.array([0.0, 0.0, 0.0])
                    WHITE = np.array([1.0, 1.0, 1.0])
                    GOLD = np.array([1.0, 0.843, 0.0])

                    if max_z == min_z:
                        colors = np.array([GOLD] * len(z_values))
                    else:
                        mid_z = (min_z + max_z) / 2
                        colors = []
                        for height in z_values:
                            if height < mid_z:
                                if mid_z == min_z:
                                    color = GOLD
                                else:
                                    ratio = (height - min_z) / (mid_z - min_z)
                                    color = BLACK * (1 - ratio) + GOLD * ratio
                            else:
                                if max_z == mid_z:
                                    color = GOLD
                                else:
                                    ratio = (height - mid_z) / (max_z - mid_z)
                                    color = GOLD * (1 - ratio) + WHITE * ratio
                            colors.append(color)
                        colors = np.array(colors)

                    self.pyvista_mesh['colors'] = colors

                self.plotter.clear()
                self.plotter.add_mesh(self.pyvista_mesh, scalars='colors', rgb=True, show_edges=False)
                self.plotter.reset_camera()
                self.plotter.show_grid() # --- MODIFIED: line_width removed ---

                self.status_label.setText(f'Loaded STL: {os.path.basename(file_path)}')
                self.convert_btn.setEnabled(False)
                self.save_stl_btn.setEnabled(True if self.stl_mesh_data is not None else False)

            except ValueError as ve:
                QMessageBox.critical(self, "Load STL Error", f"Failed to process STL mesh data. It might be malformed or empty. Details: {ve}")
                self.status_label.setText(f'Error loading STL: {ve}')
                self.pyvista_mesh = None
                self.stl_mesh_data = None
                self.plotter.clear()
                self.plotter.reset_camera()
                self.save_stl_btn.setEnabled(False)
            except Exception as e:
                QMessageBox.critical(self, "Load STL Error", f"An error occurred while loading the STL file: {e}")
                self.status_label.setText(f'Error loading STL: {e}')
                self.pyvista_mesh = None
                self.stl_mesh_data = None
                self.plotter.clear()
                self.plotter.reset_camera()
                self.save_stl_btn.setEnabled(False)


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
            print(f"DEBUG: Stripped Scale Text: '{unit_per_pixel_str}' (Length: {len(unit_per_pixel_str)})")

            max_height = float(max_height_str)
            print(f"DEBUG: Successfully converted max_height to float: {max_height}")

            unit_per_pixel = float(unit_per_pixel_str)
            print(f"DEBUG: Successfully converted unit_per_pixel to float: {unit_per_pixel}")

            invert_height = self.invert_height_checkbox.isChecked()

            img = Image.open(self.image_path).convert('L')
            img_array = np.array(img)

            height_map = img_array / 255.0 * max_height

            if invert_height:
                height_map = max_height - height_map

            rows, cols = height_map.shape

            vertices = []
            for r in range(rows):
                for c in range(cols):
                    x = c * unit_per_pixel
                    y = r * unit_per_pixel
                    z = height_map[r, c]
                    vertices.append([x, y, z])
            vertices = np.array(vertices)

            faces = []
            for r in range(rows - 1):
                for c in range(cols - 1):
                    p1 = r * cols + c
                    p2 = r * cols + (c + 1)
                    p3 = (r + 1) * cols + c
                    p4 = (r + 1) * cols + (c + 1)

                    faces.append([p1, p3, p2])
                    faces.append([p2, p3, p4])
            faces = np.array(faces)

            pyvista_faces = np.hstack((np.full((len(faces), 1), 3), faces)).flatten()
            self.pyvista_mesh = pv.PolyData(vertices, pyvista_faces)

            # --- COLORING CODE ---
            if vertices.size > 0:
                z_values = vertices[:, 2]
                min_z = np.min(z_values)
                max_z = np.max(z_values)

                BLACK = np.array([0.0, 0.0, 0.0])
                WHITE = np.array([1.0, 1.0, 1.0])
                GOLD = np.array([1.0, 0.843, 0.0])

                if max_z == min_z:
                    colors = np.array([GOLD] * len(z_values))
                else:
                    mid_z = (min_z + max_z) / 2
                    colors = []
                    for height in z_values:
                        if height < mid_z:
                            if mid_z == min_z:
                                color = GOLD
                            else:
                                ratio = (height - min_z) / (mid_z - min_z)
                                color = BLACK * (1 - ratio) + GOLD * ratio
                        else:
                            if max_z == mid_z:
                                color = GOLD
                            else:
                                ratio = (height - mid_z) / (max_z - mid_z)
                                color = GOLD * (1 - ratio) + WHITE * ratio
                        colors.append(color)
                    colors = np.array(colors)

                self.pyvista_mesh['colors'] = colors

            self.stl_mesh_data = mesh.Mesh(np.zeros(faces.shape[0], dtype=mesh.Mesh.dtype))
            for i, f in enumerate(faces):
                self.stl_mesh_data.vectors[i][0] = vertices[f[0]]
                self.stl_mesh_data.vectors[i][1] = vertices[f[1]]
                self.stl_mesh_data.vectors[i][2] = vertices[f[2]]


            self.plotter.clear()
            self.plotter.add_mesh(self.pyvista_mesh, scalars='colors', rgb=True, show_edges=False)
            self.plotter.reset_camera()
            self.plotter.show_grid() # --- MODIFIED: line_width removed ---

            self.status_label.setText('Conversion complete! Mesh displayed in viewer. You can now save the STL.')
            self.save_stl_btn.setEnabled(True)
            self.convert_btn.setEnabled(True)

        except ValueError as e:
            print(f"DEBUG: Caught ValueError: {e}")
            QMessageBox.critical(self, "Input Error", "Please enter valid numerical values for height and scale.")
            self.status_label.setText('Error: Invalid input values.')
            self.save_stl_btn.setEnabled(False)
            self.pyvista_mesh = None
            self.stl_mesh_data = None
        except Exception as e:
            print(f"DEBUG: Caught general Exception: {e}")
            QMessageBox.critical(self, "Conversion Error", f"An error occurred during conversion: {e}")
            self.status_label.setText(f'Error during conversion: {e}')
            self.save_stl_btn.setEnabled(False)
            self.pyvista_mesh = None
            self.stl_mesh_data = None


    def save_stl(self):
        if self.stl_mesh_data is None and self.pyvista_mesh is None:
            QMessageBox.warning(self, "No STL Data", "No STL data to save. Load an STL or convert an image first.")
            return

        file_dialog = QFileDialog()

        suggested_filename = "output.stl"
        if self.image_path:
            base_name = os.path.splitext(os.path.basename(self.image_path))[0]
            suggested_filename = f"{base_name}_heightmap.stl"
        elif self.pyvista_mesh and self.pyvista_mesh.path:
            base_name = os.path.splitext(os.path.basename(self.pyvista_mesh.path))[0]
            suggested_filename = f"{base_name}_loaded.stl"

        file_path, _ = file_dialog.getSaveFileName(
            self, 'Save STL', suggested_filename,
            'STL Files (*.stl)'
        )
        if file_path:
            try:
                if self.stl_mesh_data is not None:
                    self.stl_mesh_data.save(file_path)
                elif self.pyvista_mesh is not None:
                    self.pyvista_mesh.save(file_path)
                else:
                    raise ValueError("No mesh data available to save.")

                self.status_label.setText(f'STL saved to: {file_path}')
            except Exception as e:
                QMessageBox.critical(self, "Save Error", f"An error occurred while saving the STL file: {e}")
                self.status_label.setText(f'Error saving STL: {e}')

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = ImageToSTLApp()
    ex.show()
    sys.exit(app.exec_())