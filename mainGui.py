import tkinter as tk
from tkinter import ttk
from datetime import datetime
import threading
import time
import json
import os
import glob

# ====================================================================================================
# Using actual imports from the user's main.py script
# ====================================================================================================
import helper_files.mapping_jsons as Map
from helper_files.folium_map_route_generation import generate_html_maps
import helper_files.stops_enrichment_postcode as postcode_enrich
import helper_files.stops_enrichment_oas as oa_enrich
import helper_files.stops_enrichment_shops as shop_enrich
import helper_files.stops_enriched_to_db_csv as build_enrich
import helper_files.trips_enriched as trip_enrich
import helper_files.data_pipeline as data_pipeline
import helper_files.kmeans_enrichment as k_means
import helper_files.avg_weekly_frequency_per_hour_prediction as service_prediction
import helper_files.logger as logger
import atexit

# ====================================================================================================
# Main GUI Application
# ====================================================================================================
class MethodRunnerApp:
    """
    A Python GUI application to run data pipeline methods
    and track their last execution time using threading and persistent storage.
    """
    def __init__(self, root):
        self.root = root
        self.root.title("Data Pipeline Runner GUI")
        self.root.geometry("1400x800")
        self.root.configure(bg='#2c3e50')

        self.status_labels = {}
        self.running_tasks = {}
        self.last_run_data = {}
        self.json_file = "run_times.json"
        self.timer_id = None
        self.all_buttons = []
        self.log_update_active = False

        self.methods = {
            "Build Initial DB": data_pipeline.run_initial_build,
            "Generate Mapping JSONs": Map.generate_mapping_jsons,
            "Generate HTML Maps": generate_html_maps,
            "Enrich with Postcode": postcode_enrich.generate_stops_postcode,
            "Enrich with OA/LSOA": oa_enrich.generate_oas,
            "Enrich with Nearby Shops": shop_enrich.nearby_shops_enrichment,
            "Build Enriched Stops": build_enrich.write_enriched_to_db_csv,
            "Build K-means Model": k_means.kmeans_model,
            "Build Prediction Model": service_prediction.run_prediction_model,
            "Generate Enriched Trips": trip_enrich.generate_trips_enriched,
        }

        self.load_run_times()

        self.main_frame = tk.Frame(self.root, bg='#2c3e50', padx=20, pady=20, relief=tk.FLAT, bd=0)
        self.main_frame.pack(expand=True, fill=tk.BOTH)

        title_label = tk.Label(
            self.main_frame,
            text="Data Pipeline Runner",
            font=("Helvetica", 20, "bold"),
            bg='#2c3e50',
            fg='white'
        )
        title_label.pack(pady=(0, 25))

        self.button_frame = tk.Frame(self.main_frame, bg='#34495e', padx=20, pady=20, relief=tk.FLAT, bd=0)
        self.button_frame.pack(pady=(0, 20), fill=tk.X)

        run_all_button = tk.Button(
            self.button_frame,
            text="Run All Pipeline Steps",
            command=self.start_all_tasks,
            font=("Helvetica", 12, "bold"),
            bg="#e74c3c", fg="white",
            activebackground="#c0392b",
            relief=tk.FLAT,
            bd=0,
            padx=20, pady=10
        )
        run_all_button.pack(side=tk.LEFT, padx=(0, 15), pady=5)
        self.all_buttons.append(run_all_button)
        run_all_button.bind("<Enter>", lambda e, b=run_all_button: b.config(bg="#c0392b"))
        run_all_button.bind("<Leave>", lambda e, b=run_all_button: b.config(bg="#e74c3c"))

        self.content_frame = tk.Frame(self.main_frame, bg='#ecf0f1', bd=0, relief=tk.FLAT)
        self.content_frame.pack(expand=True, fill=tk.BOTH)

        self.buttons_container = tk.Frame(self.content_frame, bg='#ecf0f1', padx=10, pady=10)
        self.buttons_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.canvas = tk.Canvas(self.buttons_container, bg='#ecf0f1', bd=0, relief=tk.FLAT)
        self.scrollbar = ttk.Scrollbar(self.buttons_container, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas, bg='#ecf0f1')

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(
                scrollregion=self.canvas.bbox("all")
            )
        )

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        for method_name, method_function in self.methods.items():
            method_row_frame = tk.Frame(self.scrollable_frame, bg='#ecf0f1')
            method_row_frame.pack(fill=tk.X, pady=8)

            button = tk.Button(
                method_row_frame,
                text=f"Run {method_name}",
                command=lambda name=method_name, func=method_function: self.start_task(name, func),
                font=("Helvetica", 10, "bold"),
                bg="#3498db", fg="white",
                activebackground="#2980b9",
                relief=tk.FLAT,
                bd=0,
                padx=15, pady=8
            )
            button.pack(side=tk.LEFT, padx=(0, 15))
            self.all_buttons.append(button)

            button.bind("<Enter>", lambda e, b=button: b.config(bg="#2980b9"))
            button.bind("<Leave>", lambda e, b=button: b.config(bg="#3498db"))

            initial_text = "Not yet run"
            if method_name in self.last_run_data:
                run_data = self.last_run_data[method_name]
                if isinstance(run_data, dict):
                    timestamp = run_data.get('timestamp', 'Not found')
                    duration = run_data.get('duration', 'Not found')
                    initial_text = f"Last run at {timestamp} (took {duration})"
                else:
                    initial_text = f"Last run at {run_data}"
            
            status_label = tk.Label(
                method_row_frame,
                text=initial_text,
                anchor="w",
                justify="left",
                font=("Helvetica", 10),
                bg="#ecf0f1",
                fg="#34495e",
                padx=5
            )
            status_label.pack(side=tk.LEFT, expand=True, fill=tk.X)
            self.status_labels[method_name] = status_label

        self.log_container = tk.Frame(self.content_frame, bg='#34495e', padx=10, pady=10)
        self.log_container.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        self.log_title = tk.Label(
            self.log_container,
            text="Application Log (Inactive)",
            font=("Helvetica", 12, "bold"),
            bg='#34495e',
            fg='white'
        )
        self.log_title.pack(pady=(0, 10))

        self.log_text = tk.Text(
            self.log_container,
            bg='#2c3e50',
            fg='#ecf0f1',
            font=("Consolas", 9),
            relief=tk.FLAT,
            bd=0
        )
        
        self.log_scrollbar = ttk.Scrollbar(self.log_container, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=self.log_scrollbar.set)
        
        self.log_text.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
        self.log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.update_log_display(initial=True)

    def load_run_times(self):
        """
        Loads the last run times from a JSON file.
        """
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, 'r') as f:
                    self.last_run_data = json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                print(f"Error loading {self.json_file}: {e}")
                self.last_run_data = {}
        else:
            self.last_run_data = {}

    def save_run_times(self):
        """
        Saves the current run times to a JSON file.
        """
        try:
            with open(self.json_file, 'w') as f:
                json.dump(self.last_run_data, f, indent=4)
        except IOError as e:
            print(f"Error saving to {self.json_file}: {e}")

    def update_log_display(self, initial=False):
        """
        Reads the entire log file and displays it in the text widget.
        Auto-scrolls to the bottom only when log_update_active is True.
        """
        if not initial and not self.log_update_active:
            return

        log_file = "application_activity.log"
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                lines = f.readlines()
            
            all_lines = "".join(lines)
            
            self.log_text.config(state=tk.NORMAL)
            self.log_text.delete('1.0', tk.END)
            self.log_text.insert(tk.END, all_lines)
            
            if self.log_update_active:
                self.log_text.see(tk.END)
            
            self.log_text.config(state=tk.DISABLED)
        else:
            self.log_text.config(state=tk.NORMAL)
            self.log_text.delete('1.0', tk.END)
            self.log_text.insert(tk.END, "Waiting for log updates...")
            self.log_text.config(state=tk.DISABLED)

        if self.log_update_active:
            self.root.after(500, self.update_log_display)

    def set_all_buttons_state(self, state):
        """
        Disables or enables all buttons in the GUI.
        """
        for button in self.all_buttons:
            button['state'] = state

    def update_timer(self, method_name, start_time):
        """
        Updates the timer on the status label, showing hours, minutes, and seconds.
        """
        if method_name in self.running_tasks:
            elapsed_seconds = int(time.time() - start_time)
            hours = elapsed_seconds // 3600
            minutes = (elapsed_seconds % 3600) // 60
            seconds = elapsed_seconds % 60
            
            time_string = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            self.status_labels[method_name].config(text=f"Running ({time_string})")
            self.timer_id = self.root.after(1000, self.update_timer, method_name, start_time)

    def _cleanup_task(self, method_name):
        """
        A dedicated function to clean up the GUI state after a task is finished.
        """
        if method_name in self.running_tasks:
            del self.running_tasks[method_name]

        if self.timer_id:
            self.root.after_cancel(self.timer_id)
            self.timer_id = None

        self.set_all_buttons_state('normal')
        
        self.log_update_active = False
        self.log_title.config(text="Application Log (Inactive)")
        self.update_log_display()

    def task_executor(self, method_name, method_function):
        """
        This function runs in a separate thread.
        """
        self.set_all_buttons_state('disabled')
        start_time = time.time()
        
        self.log_update_active = True
        self.log_title.config(text="Application Log (Active)")
        self.root.after(0, self.update_log_display)

        self.root.after(0, self.update_timer, method_name, start_time)
        self.root.after(0, self.status_labels[method_name].config, {'text': "Running..."})

        try:
            method_function()

            end_time = time.time()
            duration = end_time - start_time
            duration_string = self._format_duration(duration)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.root.after(0, self.status_labels[method_name].config, {
                'text': f"Last run at {current_time} (took {duration_string})"
            })
            self.last_run_data[method_name] = {
                "timestamp": current_time,
                "duration": duration_string
            }
            self.save_run_times()

        except Exception as e:
            self.root.after(0, self.status_labels[method_name].config, {
                'text': f"ERROR - {e}"
            })
            print(f"An error occurred while running {method_name}: {e}")
        finally:
            self.root.after(0, self._cleanup_task, method_name)

    def _format_duration(self, seconds):
        """
        Formats a given duration in seconds into a human-readable HH:MM:SS string.
        """
        total_seconds = int(seconds)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def start_task(self, method_name, method_function):
        """
        Starts a new thread to run a specific method.
        """
        if method_name not in self.running_tasks:
            thread = threading.Thread(target=self.task_executor, args=(method_name, method_function))
            self.running_tasks[method_name] = thread
            thread.daemon = True
            thread.start()
        else:
            print(f"Task for {method_name} is already running.")

    def start_all_tasks(self):
        """
        Starts all methods in a new thread, sequentially.
        """
        if any(self.running_tasks):
            print("One or more tasks are already running. Please wait for them to finish.")
            return

        def run_sequence():
            for method_name, method_function in self.methods.items():
                self.start_task(method_name, method_function)
                while method_name in self.running_tasks:
                    time.sleep(0.1)

        thread = threading.Thread(target=run_sequence)
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    LOG_FILE_NAME = "application_activity.log"
    LOG_FILE_MODE = "w"
    
    try:
        logger.initialize_logger(LOG_FILE_NAME, mode=LOG_FILE_MODE)
        atexit.register(logger.close_logger)
        
        root = tk.Tk()
        app = MethodRunnerApp(root)
        root.mainloop()
    except Exception as e:
        logger.log(f"An unhandled error occurred during execution: {e}")
        raise
