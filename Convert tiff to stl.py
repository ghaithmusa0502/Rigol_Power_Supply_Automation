import tkinter as tk
from tkinter import ttk, filedialog, messagebox, colorchooser
from PIL import Image, ImageOps
import numpy as np
import trimesh
import os
import threading
from typing import Optional, Tuple, Dict, Any
from scipy.ndimage import gaussian_filter

# --- Pyglet and Trimesh Viewer Imports (with fallback) ---
# Trimesh.viewer automatically handles Pyglet if installed.
# We'll just check if trimesh can show a scene, which implies a viewer backend is available.
try:
    _test_mesh = trimesh.creation.box()
    _test_scene = trimesh.Scene(_test_mesh)
    # Attempt to use _test_scene.show(viewer_wait=False) in a separate thread if needed
    # but for simplicity, we'll just check if trimesh is generally capable.
    # A true check would involve actually trying to open a window and catching errors.
    # For now, we'll assume if trimesh is installed, its viewer might work.
    # The actual showing will be in a separate thread to not block the GUI.
    TRIMESH_VIEWER_AVAILABLE = True
except Exception:
    TRIMESH_VIEWER_AVAILABLE = False
    print("Warning: Trimesh viewer components might not be fully functional. Interactive viewing may be limited.")


class TIFFtoSTLApp:
    """A Tkinter application to convert TIFF heightmaps to 3D models and view STL files."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("TIFF to STL Converter & Viewer")
        self.root.geometry("650x950") # Slightly increased height

        # --- Data Members ---
        self.img_array: Optional[np.ndarray] = None
        self.file_path: Optional[str] = None
        self.bg_color_rgb: Optional[Tuple[int, int, int]] = (128, 128, 128) # Default to grey
        self.last_viewed_mesh: Optional[trimesh.Trimesh] = None
        # self.last_view_transform: Optional[np.ndarray] = None # Removed as custom viewer is gone

        # --- Control Variables ---
        self.conversion_mode_var = tk.StringVar(value="Heightmap")
        self.detail_var = tk.StringVar(value="Medium")
        self.remove_bg_var = tk.BooleanVar(value=False)
        self.specify_color_bg_var = tk.BooleanVar(value=False)
        self.tolerance_var = tk.StringVar(value="10")
        self.invert_output_var = tk.BooleanVar(value=False)
        self.transparency_conversion_var = tk.StringVar(value="Default")
        self.enable_smoothing_var = tk.BooleanVar(value=False)
        self.z_mirror_var = tk.BooleanVar(value=False)
        self.enable_height_color_var = tk.BooleanVar(value=False)
        self.width_var = tk.StringVar(value="")
        self.length_var = tk.StringVar(value="")
        self.units_var = tk.StringVar(value="mm")
        self.output_format_var = tk.StringVar(value="stl")
        self.stl_format_type_var = tk.StringVar(value="binary")
        self.camera_rot_x_var = tk.StringVar(value="30")
        self.camera_rot_y_var = tk.StringVar(value="-45")
        self.camera_rot_z_var = tk.StringVar(value="0")
        self.camera_distance_var = tk.StringVar(value="2.0")
        self.render_resolution_var = tk.StringVar(value="800x600")

        # --- GUI Setup ---
        self.build_gui()
        self.update_button_states()
        self.color_display_lbl.config(bg="#808080") # Set initial background color display

    def build_gui(self):
        """Builds the graphical user interface."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- TIFF to STL Conversion Section ---
        tiff_frame = ttk.LabelFrame(main_frame, text="TIFF to 3D Model Conversion", padding="10")
        tiff_frame.pack(fill=tk.BOTH, pady=5, expand=True)
        tiff_frame.columnconfigure(1, weight=1)

        row_idx = 0

        # Load and Save buttons
        self.load_tiff_btn = ttk.Button(tiff_frame, text="Load TIFF Image", command=self.load_tiff)
        self.load_tiff_btn.grid(row=row_idx, column=0, columnspan=2, pady=5, sticky='ew')
        row_idx += 1

        self.save_processed_img_btn = ttk.Button(tiff_frame, text="Save Processed Image",
                                               command=self.save_processed_image, state=tk.DISABLED)
        self.save_processed_img_btn.grid(row=row_idx, column=0, columnspan=2, pady=5, sticky='ew')
        row_idx += 1

        # Conversion Mode
        ttk.Label(tiff_frame, text="Conversion Mode:").grid(row=row_idx, column=0, sticky='w', padx=5)
        mode_frame = ttk.Frame(tiff_frame)
        ttk.Radiobutton(mode_frame, text="Heightmap", variable=self.conversion_mode_var,
                       value="Heightmap", command=self.on_conversion_mode_change).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(mode_frame, text="Extrude", variable=self.conversion_mode_var,
                       value="Extrude", command=self.on_conversion_mode_change).pack(side=tk.LEFT, padx=5)
        mode_frame.grid(row=row_idx, column=1, sticky='ew', padx=5)
        row_idx += 1

        # Detail Level
        ttk.Label(tiff_frame, text="Detail:").grid(row=row_idx, column=0, sticky='w', padx=5)
        self.detail_menu = ttk.Combobox(tiff_frame, textvariable=self.detail_var,
                                      values=["Low", "Medium", "High"], state="readonly")
        self.detail_menu.grid(row=row_idx, column=1, sticky='ew', padx=5)
        self.detail_menu.set("Medium")
        row_idx += 1

        # Height Scale
        self.height_scale_label = ttk.Label(tiff_frame, text="Height Scale:")
        self.height_scale_label.grid(row=row_idx, column=0, sticky='w', padx=5)
        self.scale_entry = ttk.Entry(tiff_frame)
        self.scale_entry.insert(0, "10.0")
        self.scale_entry.grid(row=row_idx, column=1, sticky='ew', padx=5)
        row_idx += 1

        # Base Height
        self.base_height_label = ttk.Label(tiff_frame, text="Base Height:")
        self.base_height_label.grid(row=row_idx, column=0, sticky='w', padx=5)
        self.base_height_entry = ttk.Entry(tiff_frame)
        self.base_height_entry.insert(0, "1.0")
        self.base_height_entry.grid(row=row_idx, column=1, sticky='ew', padx=5)
        row_idx += 1

        # Background Removal
        self.remove_bg_chk = ttk.Checkbutton(tiff_frame, text="Remove Background",
                                           variable=self.remove_bg_var, command=self.on_remove_bg_toggle)
        self.remove_bg_chk.grid(row=row_idx, column=0, columnspan=2, sticky='w', padx=5)
        row_idx += 1

        # Background options
        self.bg_options_frame = ttk.Frame(tiff_frame, padding="0 0 0 20")
        self.bg_options_frame.grid(row=row_idx, column=0, columnspan=2, sticky='ew', padx=5)
        self.bg_options_frame.columnconfigure(1, weight=1)

        self.specify_color_chk = ttk.Checkbutton(self.bg_options_frame, text="Specify Color:",
                                               variable=self.specify_color_bg_var, command=self.on_specify_color_toggle)
        self.specify_color_chk.grid(row=0, column=0, sticky='w')

        self.color_display_lbl = tk.Label(self.bg_options_frame, text="      ", bg="grey", relief="sunken")
        self.color_display_lbl.grid(row=0, column=1, sticky='w', padx=5)

        self.pick_color_btn = ttk.Button(self.bg_options_frame, text="Pick Color", command=self.pick_color)
        self.pick_color_btn.grid(row=0, column=2, sticky='e', padx=5)

        ttk.Label(self.bg_options_frame, text="Tolerance:").grid(row=1, column=0, sticky='w')
        self.tolerance_entry = ttk.Entry(self.bg_options_frame, textvariable=self.tolerance_var)
        self.tolerance_entry.grid(row=1, column=1, sticky='ew')
        self.tolerance_entry.insert(0, "10")

        self.bg_options_frame.grid_remove()
        row_idx += 1

        # Additional Options
        self.invert_output_chk = ttk.Checkbutton(tiff_frame, text="Invert Output", variable=self.invert_output_var)
        self.invert_output_chk.grid(row=row_idx, column=0, columnspan=2, sticky='w', padx=5)
        row_idx += 1

        # Transparency handling
        ttk.Label(tiff_frame, text="Transparency:").grid(row=row_idx, column=0, sticky='w', padx=5)
        trans_frame = ttk.Frame(tiff_frame)
        ttk.Radiobutton(trans_frame, text="Default", variable=self.transparency_conversion_var,
                       value="Default").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(trans_frame, text="To Black", variable=self.transparency_conversion_var,
                       value="Black").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(trans_frame, text="To White", variable=self.transparency_conversion_var,
                       value="White").pack(side=tk.LEFT, padx=5)
        trans_frame.grid(row=row_idx, column=1, sticky='ew', padx=5)
        row_idx += 1

        # Smoothing, Mirror, Height Color
        self.smoothing_chk = ttk.Checkbutton(tiff_frame, text="Enable Smoothing", variable=self.enable_smoothing_var)
        self.smoothing_chk.grid(row=row_idx, column=0, columnspan=2, sticky='w', padx=5)
        row_idx += 1

        self.z_mirror_chk = ttk.Checkbutton(tiff_frame, text="Mirror Z-Axis", variable=self.z_mirror_var)
        self.z_mirror_chk.grid(row=row_idx, column=0, columnspan=2, sticky='w', padx=5)
        row_idx += 1

        self.enable_height_color_chk = ttk.Checkbutton(tiff_frame, text="Color by Height (low-midrange- high points, Black-Gold-white)",
                                             variable=self.enable_height_color_var)
        self.enable_height_color_chk.grid(row=row_idx, column=0, columnspan=2, sticky='w', padx=5)
        row_idx += 1

        # Model Dimensions
        size_frame = ttk.LabelFrame(tiff_frame, text="Model Dimensions", padding="5")
        size_frame.grid(row=row_idx, column=0, columnspan=2, pady=5, sticky='ew')
        size_frame.columnconfigure(1, weight=1)
        row_idx += 1

        ttk.Label(size_frame, text="Width:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.width_entry = ttk.Entry(size_frame, textvariable=self.width_var)
        self.width_entry.grid(row=0, column=1, sticky='ew', padx=5, pady=2)

        ttk.Label(size_frame, text="Length:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.length_entry = ttk.Entry(size_frame, textvariable=self.length_var)
        self.length_entry.grid(row=1, column=1, sticky='ew', padx=5, pady=2)

        ttk.Label(size_frame, text="Units:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        self.units_menu = ttk.Combobox(size_frame, textvariable=self.units_var,
                                     values=["mm", "cm", "inch", "um"], state="readonly")
        self.units_menu.grid(row=2, column=1, sticky='ew', padx=5, pady=2)
        self.units_menu.set("mm")

        # Output Format
        ttk.Label(tiff_frame, text="Output Format:").grid(row=row_idx, column=0, sticky='w', padx=5)
        self.output_format_menu = ttk.Combobox(tiff_frame, textvariable=self.output_format_var,
                                             values=["stl", "obj", "glb", "ply"], state="readonly")
        self.output_format_menu.grid(row=row_idx, column=1, sticky='ew', padx=5)
        self.output_format_menu.set("stl")
        self.output_format_menu.bind("<<ComboboxSelected>>", self.on_output_format_change)
        row_idx += 1

        # STL Format options
        self.stl_format_frame = ttk.Frame(tiff_frame)
        ttk.Label(self.stl_format_frame, text="STL Type:").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(self.stl_format_frame, text="Binary", variable=self.stl_format_type_var,
                       value="binary").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(self.stl_format_frame, text="ASCII", variable=self.stl_format_type_var,
                       value="ascii").pack(side=tk.LEFT, padx=5)
        self.stl_format_frame.grid(row=row_idx, column=0, columnspan=2, sticky='w', padx=20)
        self.stl_format_frame.grid_remove()
        row_idx += 1

        # Convert Button
        self.convert_btn = ttk.Button(tiff_frame, text="Convert to 3D Model", command=self.start_conversion_thread)
        self.convert_btn.grid(row=row_idx, column=0, columnspan=2, pady=10, sticky='ew')
        row_idx += 1

        # --- 3D Model Viewer Section ---
        viewer_frame = ttk.LabelFrame(main_frame, text="3D Model Viewer & Render", padding="10")
        viewer_frame.pack(fill=tk.X, pady=5)

        self.load_stl_btn = ttk.Button(viewer_frame, text="Load & View Model (Interactive)", command=self.load_and_view_model)
        self.load_stl_btn.pack(fill=tk.X, pady=5)
        if not TRIMESH_VIEWER_AVAILABLE:
            self.load_stl_btn.config(state=tk.DISABLED, text="Load & View Model (Viewer Unavailable)")
            messagebox.showwarning("Viewer Warning", "Trimesh viewer dependencies (like Pyglet) not found. Interactive viewing disabled. "
                                   "You can still convert and save models.")

        # Camera Controls
        camera_frame = ttk.LabelFrame(viewer_frame, text="Static Render Controls", padding="5")
        camera_frame.pack(fill=tk.X, pady=5)
        camera_frame.columnconfigure(1, weight=1)


        ttk.Label(camera_frame, text="Rotate X:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.camera_rot_x_entry = ttk.Entry(camera_frame, textvariable=self.camera_rot_x_var)
        self.camera_rot_x_entry.grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        self.camera_rot_x_var.trace('w', self.update_camera_display)
        print("Trace added for camera_rot_x_var") # Add this line

        ttk.Label(camera_frame, text="Rotate Y:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.camera_rot_y_entry = ttk.Entry(camera_frame, textvariable=self.camera_rot_y_var)
        self.camera_rot_y_entry.grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        self.camera_rot_y_var.trace('w', self.update_camera_display)
        print("Trace added for camera_rot_y_var") # Add this line

        ttk.Label(camera_frame, text="Rotate Z:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        self.camera_rot_z_entry = ttk.Entry(camera_frame, textvariable=self.camera_rot_z_var)
        self.camera_rot_z_entry.grid(row=2, column=1, sticky='ew', padx=5, pady=2)
        self.camera_rot_z_var.trace('w', self.update_camera_display)
        print("Trace added for camera_rot_z_var") # Add this line

        ttk.Label(camera_frame, text="Distance:").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        self.camera_distance_entry = ttk.Entry(camera_frame, textvariable=self.camera_distance_var)
        self.camera_distance_entry.grid(row=3, column=1, sticky='ew', padx=5, pady=2)
        self.camera_distance_var.trace('w', self.update_camera_display)
        print("Trace added for camera_distance_var") # Add this line

        ttk.Label(camera_frame, text="Resolution:").grid(row=4, column=0, sticky='w', padx=5, pady=2)
        self.render_resolution_entry = ttk.Entry(camera_frame, textvariable=self.render_resolution_var)
        self.render_resolution_entry.grid(row=4, column=1, sticky='ew', padx=5, pady=2)

        # Real-time Camera Coordinates Display
        coord_display_frame = ttk.LabelFrame(camera_frame, text="Current Camera Position", padding="5")
        coord_display_frame.grid(row=5, column=0, columnspan=2, sticky='ew', pady=5)

        self.current_coords_label = ttk.Label(coord_display_frame, text="Rotation X: 30° | Rotation Y: -45° | Rotation Z: 0° | Distance: 2.0",
                                            font=('Courier', 9), foreground='blue')
        self.current_coords_label.pack(anchor='w')
        # Render buttons
        self.save_3d_render_btn = ttk.Button(viewer_frame, text="Save Render (from Controls)",
                                           command=self.save_3d_viewer_render, state=tk.DISABLED)
        self.save_3d_render_btn.pack(fill=tk.X, pady=2)

        self.save_interactive_render_btn = ttk.Button(viewer_frame, text="Save Last Interactive View Render",
                                                    command=self.save_last_interactive_render, state=tk.DISABLED)
        self.save_interactive_render_btn.pack(fill=tk.X, pady=2)

        # --- Status Bar ---
        self.progress_bar = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress_bar.pack(fill=tk.X, pady=5)
        self.status_label = ttk.Label(main_frame, text="Load a TIFF file to begin.", anchor='w')
        self.status_label.pack(fill=tk.X, pady=5)

        # Initialize UI state
        self.on_conversion_mode_change()
        self.on_remove_bg_toggle()
        self.on_output_format_change()
        self.update_camera_display() # Initialize camera coordinate display


    def update_status(self, text: str):
        """Updates the status label."""
        self.status_label.config(text=text)
        self.root.update_idletasks()

    def update_button_states(self):
        """Updates button states based on loaded data."""
        image_loaded = self.img_array is not None
        mesh_loaded = self.last_viewed_mesh is not None
        # transform_captured = self.last_view_transform is not None # Removed

        self.convert_btn.config(state=tk.NORMAL if image_loaded else tk.DISABLED)
        self.save_processed_img_btn.config(state=tk.NORMAL if image_loaded else tk.DISABLED)
        
        # Only enable render buttons if a mesh is loaded AND viewer is available
        self.save_3d_render_btn.config(state=tk.NORMAL if mesh_loaded and TRIMESH_VIEWER_AVAILABLE else tk.DISABLED)
        self.save_interactive_render_btn.config(state=tk.NORMAL if mesh_loaded and TRIMESH_VIEWER_AVAILABLE else tk.DISABLED)


    def start_progress(self):
        """Starts the progress bar."""
        self.progress_bar.start(10)

    def stop_progress(self):
        """Stops the progress bar."""
        self.progress_bar.stop()
        self.progress_bar['value'] = 0

    def on_conversion_mode_change(self):
        """Handles conversion mode changes."""
        mode = self.conversion_mode_var.get()
        if mode == "Heightmap":
            self.height_scale_label.config(text="Height Scale:")
            self.base_height_entry.config(state=tk.NORMAL)
        else:
            self.height_scale_label.config(text="Extrusion Depth:")
            self.base_height_entry.config(state=tk.DISABLED)

    def on_remove_bg_toggle(self):
        """Handles background removal toggle."""
        if self.remove_bg_var.get():
            self.bg_options_frame.grid()
        else:
            self.bg_options_frame.grid_remove()
        self.on_specify_color_toggle()

    def on_specify_color_toggle(self):
        """Handles color specification toggle."""
        is_enabled = self.remove_bg_var.get() and self.specify_color_bg_var.get()
        self.pick_color_btn.config(state=tk.NORMAL if is_enabled else tk.DISABLED)

    def pick_color(self):
        """Opens color picker dialog."""
        color_code = colorchooser.askcolor(title="Choose background color")
        if color_code[0]:
            self.bg_color_rgb = tuple(map(int, color_code[0]))
            self.color_display_lbl.config(bg=color_code[1])

    def on_output_format_change(self, event=None):
        """Handles output format changes."""
        if self.output_format_var.get() == "stl":
            self.stl_format_frame.grid()
        else:
            self.stl_format_frame.grid_remove()

    def update_camera_display(self, *args):
        """Updates the camera coordinate display in real-time."""
        print(f"update_camera_display called. Args: {args}") # Add this line
        try:
            rot_x = self.camera_rot_x_var.get() or "0"
            rot_y = self.camera_rot_y_var.get() or "0"
            rot_z = self.camera_rot_z_var.get() or "0"
            distance = self.camera_distance_var.get() or "0"

            coord_text = f"Rotation X: {float(rot_x):.1f}° | Rotation Y: {float(rot_y):.1f}° | Rotation Z: {float(rot_z):.1f}° | Distance: {float(distance):.2f}"
            self.current_coords_label.config(text=coord_text)
            print(f"Updated label: {coord_text}") # Add this line
        except ValueError:
            self.current_coords_label.config(text="Invalid Camera Input")
            print("Invalid Camera Input detected.") # Add this line


    def load_tiff(self):
        """Loads a TIFF image file."""
        file_path = filedialog.askopenfilename(
            title="Select TIFF Image",
            filetypes=[("TIFF files", "*.tif;*.tiff"), ("All files", "*.*")]
        )
        if not file_path:
            return

        self.update_status(f"Loading {os.path.basename(file_path)}...")
        try:
            img = Image.open(file_path)

            # Handle transparency (RGBA to RGB)
            if img.mode == 'RGBA':
                trans_opt = self.transparency_conversion_var.get()
                if trans_opt == "Black":
                    new_img = Image.new("RGB", img.size, (0, 0, 0))
                    new_img.paste(img, (0, 0), img) # Use alpha channel as mask
                    img = new_img
                elif trans_opt == "White":
                    new_img = Image.new("RGB", img.size, (255, 255, 255))
                    new_img.paste(img, (0, 0), img) # Use alpha channel as mask
                    img = new_img
                else: # Default: just convert to RGB, alpha is lost
                    img = img.convert('RGB')
            elif img.mode != 'RGB':
                # Convert other modes like 'P', 'L', etc. to RGB for consistent processing
                img = img.convert('RGB')

            # Remove background color if specified
            if self.remove_bg_var.get() and self.specify_color_bg_var.get() and self.bg_color_rgb:
                img = self._remove_background_color(img, self.bg_color_rgb, int(self.tolerance_var.get()))
            
            # Convert to grayscale after background removal
            img = img.convert('L')

            # Adjust detail level (resizing)
            # The original code only resizes for "Low" detail, "High" keeps original.
            # This logic needs to be consistent. "Medium" could also resize if desired.
            if self.detail_var.get() == "Low":
                img = img.resize((img.width // 2, img.height // 2), Image.LANCZOS)
            elif self.detail_var.get() == "Medium":
                 # For medium, you might choose a slight reduction or keep as is.
                 # Keeping original for now to ensure quality.
                 pass
            elif self.detail_var.get() == "High":
                pass # Keep original size for high detail


            self.img_array = np.array(img)

            # Invert if requested (apply to grayscale array)
            if self.invert_output_var.get():
                self.img_array = 255 - self.img_array

            self.file_path = file_path
            self.update_status(f"Loaded: {os.path.basename(file_path)} ({self.img_array.shape[1]}x{self.img_array.shape[0]})")

        except Exception as e:
            messagebox.showerror("Loading Error", f"Failed to load image: {e}")
            self.img_array = None
            self.update_status("Loading failed.")
        finally:
            self.update_button_states()

    def _remove_background_color(self, image: Image.Image, bg_color: Tuple[int, int, int], tolerance: int) -> Image.Image:
        """Removes background color from image by making it transparent."""
        img = image.convert("RGBA")
        data = np.array(img)
        rgb = data[:, :, :3]
        alpha = data[:, :, 3]

        # Calculate Euclidean distance for color difference
        # This is more robust than simple channel-wise difference
        diff = np.sqrt(np.sum((rgb - np.array(bg_color).reshape(1, 1, 3))**2, axis=-1))
        
        # Set alpha to 0 for pixels close to background color
        alpha[diff < tolerance] = 0
        data[:, :, 3] = alpha

        return Image.fromarray(data, "RGBA")


    def save_processed_image(self):
        """Saves the processed image."""
        if self.img_array is None:
            messagebox.showwarning("No Image", "No image loaded to save.")
            return

        save_path = filedialog.asksaveasfilename(
            title="Save Processed Image",
            defaultextension=".png",
            filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"), ("All files", "*.*")]
        )
        if not save_path:
            return

        try:
            # Convert the numpy array back to an Image object (ensure it's L mode for grayscale)
            Image.fromarray(self.img_array.astype(np.uint8), 'L').save(save_path)
            messagebox.showinfo("Success", f"Image saved to {save_path}")
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save image: {e}")

    def start_conversion_thread(self):
        """Starts the conversion process in a separate thread."""
        if self.img_array is None:
            messagebox.showwarning("No Image", "Load a TIFF image first.")
            return

        try:
            height_scale = float(self.scale_entry.get())
            base_height = float(self.base_height_entry.get())
            if height_scale <= 0 or base_height < 0:
                 messagebox.showerror("Input Error", "Height Scale must be positive and Base Height non-negative.")
                 return
        except ValueError:
            messagebox.showerror("Input Error", "Invalid height scale or base height values. Please enter numbers.")
            return

        output_format = self.output_format_var.get()
        save_path = filedialog.asksaveasfilename(
            title="Save 3D Model",
            defaultextension=f".{output_format}",
            filetypes=[(f"{output_format.upper()} files", f"*.{output_format}"), ("All files", "*.*")]
        )
        if not save_path:
            return

        self.start_progress()
        self.update_status("Converting to 3D model...")

        # Disable UI during conversion
        self.convert_btn.config(state=tk.DISABLED)
        self.load_tiff_btn.config(state=tk.DISABLED)
        self.load_stl_btn.config(state=tk.DISABLED) # Also disable model viewer during conversion

        threading.Thread(
            target=self._convert_heightmap_to_stl,
            args=(self.img_array.copy(), height_scale, base_height, save_path),
            daemon=True
        ).start()

    def _convert_heightmap_to_stl(self, img_array: np.ndarray, height_scale: float, base_height: float, save_path: str):
        """Converts heightmap to 3D model using trimesh."""
        try:
            # Apply smoothing if enabled
            if self.enable_smoothing_var.get():
                # Gaussian filter expects float input and returns float
                img_array = gaussian_filter(img_array.astype(np.float32), sigma=1.0)
                # Normalize back to 0-255 if smoothing introduced values outside this range
                img_array = np.clip(img_array, 0, 255)

            # Normalize heightmap values to 0-1 range
            height_values = img_array.astype(np.float32) / 255.0

            # Apply height scale and base height
            if self.conversion_mode_var.get() == "Heightmap":
                z_values = height_values * height_scale + base_height
            else:  # Extrude mode
                threshold_value = 128 / 255.0 # normalized threshold
                z_values = np.where(height_values > threshold_value, height_scale + base_height, base_height)

            # Mirror Z-axis if requested
            if self.z_mirror_var.get():
                # To mirror, calculate the new range for z_values
                min_z = base_height # base is always at base_height
                max_z = height_scale + base_height if self.conversion_mode_var.get() == "Heightmap" else np.max(z_values)
                z_range = max_z - min_z
                
                # Invert height within the height_scale range, then add base_height
                if self.conversion_mode_var.get() == "Heightmap":
                     z_values = (max_z - (z_values - min_z))
                else: # Extrude mode needs careful handling for binary mirror
                    # If z_values are effectively binary (base_height or height_scale + base_height)
                    # then mirroring means swapping these two states.
                    temp_z_values = z_values.copy()
                    z_values[temp_z_values == (height_scale + base_height)] = base_height
                    z_values[temp_z_values == base_height] = (height_scale + base_height)


            # Get dimensions for scaling
            nrows, ncols = img_array.shape
            
            # Calculate desired physical dimensions
            try:
                target_width = float(self.width_var.get()) if self.width_var.get().strip() else ncols
            except ValueError:
                target_width = ncols
                messagebox.showwarning("Input Warning", "Invalid Width value. Using image width.")

            try:
                target_length = float(self.length_var.get()) if self.length_var.get().strip() else nrows
            except ValueError:
                target_length = nrows
                messagebox.showwarning("Input Warning", "Invalid Length value. Using image height.")

            # Create the mesh using trimesh.creation.make_heightfield
            # make_heightfield expects Z_values (heights), a grid (x,y)
            # We need to create a meshgrid that represents the scaled X, Y coordinates
            x_scale = target_width / ncols
            y_scale = target_length / nrows
            
            x_coords = np.arange(ncols) * x_scale
            y_coords = np.arange(nrows) * y_scale

            # Pass the height data and scale directly to make_heightfield
            # make_heightfield creates a watertight mesh with a base
            mesh = trimesh.creation.make_heightfield(
                Z=z_values,
                face_type='triangles', # Default, explicit for clarity
                height=height_scale, # This parameter refers to the total height of the heightfield itself,
                                     # not the final scaled Z. We control Z with z_values.
                                     # Setting it to None here means trimesh won't rescale Z_values internally.
                                     # The Z_values passed already have the desired scale.
                # Use pitch to control the spacing between vertices, which corresponds to the unit scaling.
                pitch=[x_scale, y_scale]
            )
            
            # The make_heightfield typically creates the mesh with the base at Z=0.
            # If base_height was already added to z_values, this is fine.
            # If the original image was inverted (white=low, black=high) and now it's black=low, white=high for Z
            # the Z-mirroring has to happen here.
            
            # Correcting the base height if make_heightfield always starts at 0
            # make_heightfield generates vertices from [0, 0] to [ncols*x_scale, nrows*y_scale] for X, Y
            # and Z values are directly taken from the Z array.
            # So, our z_values already include base_height, no further adjustment is needed here.

            # Apply vertex colors if height coloring is enabled
            if self.enable_height_color_var.get() and mesh.vertices is not None:
                # Black (low) -> Gold (mid) -> Blue (high) gradient
                z_heights = mesh.vertices[:, 2] # Get Z coordinates of all vertices
                z_min, z_max = np.min(z_heights), np.max(z_heights)

                if z_max > z_min:
                    normalized_heights = (z_heights - z_min) / (z_max - z_min)
                    colors = np.zeros((len(mesh.vertices), 4), dtype=np.uint8)
                    colors[:, 3] = 255  # Alpha channel full opacity

                    # Define key colors
                    black = np.array([0, 0, 0])
                    gold = np.array([218, 165, 32]) # Goldenrod
                    white = np.array([255, 255, 255]) # White

                    for i, h in enumerate(normalized_heights):
                        if h <= 0.5:
                            # Interpolate from black to gold
                            t = h * 2
                            colors[i, :3] = (black * (1 - t) + gold * t).astype(np.uint8)
                        else:
                            # Interpolate from gold to blue
                            t = (h - 0.5) * 2
                            colors[i, :3] = (gold * (1 - t) + white  * t).astype(np.uint8)
                    mesh.visual.vertex_colors = colors
                else:
                    print("Warning: Z-range is zero, cannot apply height-based coloring.")


            # Trimesh handles watertightness and manifold properties reasonably well for heightfields
            # However, for general meshes loaded, it's good to call repair methods.
            mesh.fix_normals() # Ensure normals are consistent
            mesh.remove_duplicate_faces()
            mesh.remove_degenerate_faces()
            mesh.fill_holes() # Fill any holes that might exist (though heightfield should be watertight)

            if not mesh.is_watertight:
                print("Warning: Generated mesh is not watertight.")
                self.root.after(0, lambda: messagebox.showwarning("Mesh Warning", "The generated mesh might not be watertight. This can affect 3D printing."))
            if not mesh.is_winding_consistent:
                 print("Warning: Generated mesh is not winding consistent.")
                 self.root.after(0, lambda: messagebox.showwarning("Mesh Warning", "The generated mesh is not winding consistent."))

            # Save the mesh
            output_format = self.output_format_var.get()

            if output_format == "stl":
                file_type = self.stl_format_type_var.get()
                if file_type == "ascii":
                    mesh.export(save_path, file_type='stl_ascii')
                else:
                    mesh.export(save_path, file_type='stl')
            else:
                mesh.export(save_path, file_type=output_format)

            # Update UI on main thread
            self.root.after(0, self._conversion_complete, save_path, mesh)

        except Exception as e:
            error_msg = f"Conversion failed: {str(e)}"
            self.root.after(0, self._conversion_error, error_msg)

    def _conversion_complete(self, save_path: str, mesh: trimesh.Trimesh):
            """Called when conversion completes successfully."""
            self.stop_progress()
            self.convert_btn.config(state=tk.NORMAL)
            self.load_tiff_btn.config(state=tk.NORMAL)
            self.load_stl_btn.config(state=tk.NORMAL) # Re-enable model viewer

            # Store the mesh for viewing
            self.last_viewed_mesh = mesh
            self.update_button_states()

            file_size = os.path.getsize(save_path) / 1024  # KB
            self.update_status(f"Conversion complete! Saved to {os.path.basename(save_path)} ({file_size:.1f} KB)")
            messagebox.showinfo("Success", f"3D model saved successfully!\n\nFile: {save_path}\nSize: {file_size:.1f} KB")

    def _conversion_error(self, error_msg: str):
            """Called when conversion fails."""
            self.stop_progress()
            self.convert_btn.config(state=tk.NORMAL)
            self.load_tiff_btn.config(state=tk.NORMAL)
            self.load_stl_btn.config(state=tk.NORMAL) # Re-enable model viewer
            self.update_status(error_msg)
            messagebox.showerror("Conversion Error", error_msg)

    def load_and_view_model(self):
        """Loads and views a 3D model file using trimesh's built-in viewer."""
        if not TRIMESH_VIEWER_AVAILABLE:
            messagebox.showerror("Viewer Not Available", "Trimesh viewer components (like Pyglet) are not installed or not working.")
            return

        file_path = filedialog.askopenfilename(
            title="Select 3D Model",
            filetypes=[
                ("STL files", "*.stl"),
                ("OBJ files", "*.obj"),
                ("PLY files", "*.ply"),
                ("GLB files", "*.glb"),
                ("All files", "*.*")
            ]
        )
        if not file_path:
            return

        self.update_status(f"Loading 3D model: {os.path.basename(file_path)}...")

        # Disable UI during loading/viewing
        self.load_stl_btn.config(state=tk.DISABLED)
        self.convert_btn.config(state=tk.DISABLED)

        def _load_and_show():
            try:
                mesh = trimesh.load(file_path)

                if isinstance(mesh, trimesh.Scene):
                    # If it's a scene, get the combined mesh
                    mesh = mesh.to_geometry()
                
                # Store the mesh for rendering
                self.last_viewed_mesh = mesh
                
                self.root.after(0, lambda: self.update_button_states()) # Update UI after mesh is loaded
                
                # Display model info
                info = f"Loaded: {os.path.basename(file_path)} - {len(mesh.vertices)} vertices, {len(mesh.faces)} faces"
                self.root.after(0, lambda: self.update_status(info))

                # Show interactive viewer
                # Running trimesh.show() in a separate thread is crucial to not block Tkinter
                self.root.after(0, lambda: self.update_status("Opening interactive viewer... (may take a moment)"))
                scene = trimesh.Scene([mesh])
                scene.show() # This call blocks until the viewer is closed
                
                # Re-enable buttons after viewer is closed
                self.root.after(0, lambda: self.update_status(f"Interactive viewer closed. {info}"))
                self.root.after(0, lambda: self.update_button_states())
                self.root.after(0, lambda: self.load_stl_btn.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.convert_btn.config(state=tk.NORMAL))


            except Exception as e:
                error_msg = f"Failed to load or display 3D model: {e}"
                self.root.after(0, lambda: self.update_status(error_msg))
                self.root.after(0, lambda: messagebox.showerror("Loading Error", error_msg))
                self.root.after(0, lambda: self.update_button_states())
                self.root.after(0, lambda: self.load_stl_btn.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.convert_btn.config(state=tk.NORMAL))


        threading.Thread(target=_load_and_show, daemon=True).start()


    def save_3d_viewer_render(self):
        """Saves a render using the manual camera controls."""
        if self.last_viewed_mesh is None:
            messagebox.showwarning("No Mesh", "No 3D model loaded yet to render.")
            return
        if not TRIMESH_VIEWER_AVAILABLE:
            messagebox.showerror("Viewer Not Available", "Trimesh viewer components are not installed or not working.")
            return

        save_path = filedialog.asksaveasfilename(
            title="Save Render",
            defaultextension=".png",
            filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"), ("All files", "*.*")]
        )
        if not save_path:
            return

        self.update_status("Generating render from controls...")
        self.save_3d_render_btn.config(state=tk.DISABLED) # Disable to prevent multiple clicks

        def _render():
            try:
                rot_x = float(self.camera_rot_x_var.get())
                rot_y = float(self.camera_rot_y_var.get())
                rot_z = float(self.camera_rot_z_var.get())
                distance = float(self.camera_distance_var.get())

                resolution_str = self.render_resolution_var.get()
                if 'x' in resolution_str:
                    width, height = map(int, resolution_str.split('x'))
                else:
                    width, height = 800, 600

                scene = trimesh.Scene([self.last_viewed_mesh])
                
                # Calculate bounding box for the mesh to center the camera
                bounds = self.last_viewed_mesh.bounds
                center = self.last_viewed_mesh.centroid
                
                # Calculate scale factor for orbit distance
                scale = np.max(bounds[1] - bounds[0]) / 2.0 # Half the max dimension

                # Create an orbit camera
                # `trimesh.scene.cameras.look_at` is a good way to define camera transform
                # from a target point, eye position, and up vector.
                # Here, we'll build the transformation manually for more direct control with Euler angles.

                # Start with an identity transform
                camera_transform = np.eye(4)

                # Translate camera away from origin along Z axis
                camera_transform[2, 3] = distance * scale

                # Apply rotations (Z-Y-X order for common CAD/viewer conventions)
                # First, rotate around X (pitch)
                Rx = trimesh.transformations.rotation_matrix(np.radians(rot_x), [1, 0, 0])
                # Then rotate around Y (yaw)
                Ry = trimesh.transformations.rotation_matrix(np.radians(rot_y), [0, 1, 0])
                # Then rotate around Z (roll, less common for orbit)
                Rz = trimesh.transformations.rotation_matrix(np.radians(rot_z), [0, 0, 1])

                # Combine rotations
                # Order matters: typically Rz @ Ry @ Rx for external rotations
                combined_rotation = Rz @ Ry @ Rx

                # Apply rotation to the camera transform
                camera_transform = combined_rotation @ camera_transform

                # Translate the whole scene to bring the model to the center of view
                # This inverse transform effectively moves the camera around the model's center
                translation_to_center = trimesh.transformations.translation_matrix(-center)
                
                # Apply the camera transform *after* the scene translation to place the camera relative to the scene's centered origin
                final_camera_transform = camera_transform @ translation_to_center
                
                # Render the scene
                png_data = scene.save_image(resolution=(width, height), camera_transform=final_camera_transform)

                with open(save_path, 'wb') as f:
                    f.write(png_data)

                self.root.after(0, lambda: self.update_status(f"Render saved: {os.path.basename(save_path)}"))
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Render saved to {save_path}"))

            except Exception as e:
                error_msg = f"Failed to save render: {e}"
                self.root.after(0, lambda: self.update_status(error_msg))
                self.root.after(0, lambda: messagebox.showerror("Render Error", error_msg))
            finally:
                self.root.after(0, lambda: self.save_3d_render_btn.config(state=tk.NORMAL))


        threading.Thread(target=_render, daemon=True).start()


    def save_last_interactive_render(self):
        """Saves a render from a default view, as interactive camera state cannot be easily captured."""
        if self.last_viewed_mesh is None:
            messagebox.showwarning("No Mesh", "No 3D model loaded yet.")
            return
        if not TRIMESH_VIEWER_AVAILABLE:
            messagebox.showerror("Viewer Not Available", "Trimesh viewer components are not installed or not working.")
            return

        save_path = filedialog.asksaveasfilename(
            title="Save Interactive View Render",
            defaultextension=".png",
            filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"), ("All files", "*.*")]
        )
        if not save_path:
            return

        self.update_status("Generating render from last interactive view...")
        self.save_interactive_render_btn.config(state=tk.DISABLED)

        def _render_interactive():
            try:
                resolution_str = self.render_resolution_var.get()
                if 'x' in resolution_str:
                    width, height = map(int, resolution_str.split('x'))
                else:
                    width, height = 800, 600

                scene = trimesh.Scene([self.last_viewed_mesh])
                
                # Trimesh's save_image with no camera_transform argument
                # uses a default camera, which is the closest we can get to
                # a generic "last view" without direct viewer integration.
                png_data = scene.save_image(resolution=(width, height))

                with open(save_path, 'wb') as f:
                    f.write(png_data)

                self.root.after(0, lambda: self.update_status(f"Interactive view render saved: {os.path.basename(save_path)}"))
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Interactive view render saved to {save_path}"))

            except Exception as e:
                error_msg = f"Failed to save interactive render: {e}"
                self.root.after(0, lambda: self.update_status(error_msg))
                self.root.after(0, lambda: messagebox.showerror("Render Error", error_msg))
            finally:
                self.root.after(0, lambda: self.save_interactive_render_btn.config(state=tk.NORMAL))

        threading.Thread(target=_render_interactive, daemon=True).start()


def main():
    """Main application entry point."""
    root = tk.Tk()
    app = TIFFtoSTLApp(root)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nApplication interrupted by user.")
    except Exception as e:
        print(f"Application error: {e}")
        messagebox.showerror("Application Error", f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()