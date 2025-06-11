# steam_fetcher/gui.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, Scale, HORIZONTAL, Label
import asyncio
import threading
import os

# Import all handlers/scrapers
from .scraper import run_full_scrape
from .listed_scraper import ListedGameScraper
from .data_handler import DatabaseHandler
from .db_inserter import insert_csv_to_db

class AppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Steam Game Fetcher")
        self.root.geometry("450x300")

        style = ttk.Style()
        style.theme_use('clam')

        main_frame = ttk.Frame(self.root, padding="10 10 10 10")
        main_frame.pack(expand=True, fill=tk.BOTH)

        # --- Option 2 (Run Full Steam Data Scrape) - Moved to Top ---
        self.option2_button = ttk.Button(
            main_frame,
            text="Scrape All Game Data to CSV", # Renamed for clarity
            command=self.start_full_scrape_thread
        )
        self.option2_button.pack(pady=5, fill=tk.X)

        # --- Option 4 (Insert CSV to DB) - Moved to Second ---
        self.option4_button = ttk.Button(
            main_frame,
            text="Upload CSV to Azure SQL (DataValidation DB)", # Renamed for clarity
            command=self.start_db_insert_thread
        )
        self.option4_button.pack(pady=5, fill=tk.X)

        # --- Separator ---
        separator = ttk.Separator(main_frame, orient='horizontal')
        separator.pack(fill='x', pady=10, padx=5)

        # --- Option 1 (Get Steam Data for Listed Games) ---
        self.option1_button = ttk.Button(
            main_frame,
            text="Scrape Listed Games from CSV to New CSV", # Renamed for clarity
            command=self.start_listed_scrape_thread
        )
        self.option1_button.pack(pady=5, fill=tk.X)

        # --- Option 3 (Fetch Titles from DB & Store) ---
        self.option3_button = ttk.Button(
            main_frame,
            text="Process DB Titles to MongoDB (Placeholder)", # Renamed for clarity
            command=self.start_db_process_thread
        )
        self.option3_button.pack(pady=5, fill=tk.X)

        # --- Status Label ---
        self.status_label = ttk.Label(main_frame, text="Ready", wraplength=400)
        self.status_label.pack(pady=10, fill=tk.X)

        # --- Input File Label ---
        self.input_file_label = ttk.Label(main_frame, text="Input CSV: Not selected", wraplength=400)
        self.input_file_label.pack(pady=5, fill=tk.X)

        # --- Progress Bar ---
        self.progress_label = ttk.Label(main_frame, text="Progress: 0/0 (0.0%)")
        self.progress_label.pack(pady=2, fill=tk.X)
        self.progress_bar = ttk.Progressbar(main_frame, orient=HORIZONTAL, length=300, mode='determinate')
        self.progress_bar.pack(pady=5, fill=tk.X)

        self.active_thread = None

    def _disable_buttons(self):
        """Disables all action buttons."""
        self.option1_button.config(state=tk.DISABLED)
        self.option2_button.config(state=tk.DISABLED)
        self.option3_button.config(state=tk.DISABLED)
        self.option4_button.config(state=tk.DISABLED)

    def _enable_buttons(self):
        """Enables all action buttons."""
        self.option1_button.config(state=tk.NORMAL)
        self.option2_button.config(state=tk.NORMAL)
        self.option3_button.config(state=tk.NORMAL)
        self.option4_button.config(state=tk.NORMAL)
        self.active_thread = None
        self.update_progress(0, 0)

    def update_status(self, message):
        """Updates the status label from a thread."""
        self.root.after(0, lambda: self.status_label.config(text=message))

    def update_progress(self, current, total):
        """Updates the progress bar and label from a thread."""
        if total > 0:
            percentage = (current / total) * 100
            self.progress_label.config(text=f"Progress: {current}/{total} ({percentage:.1f}%)")
            self.progress_bar['maximum'] = total
            self.progress_bar['value'] = current
        else:
            self.progress_label.config(text="Progress: 0/0 (0.0%)")
            self.progress_bar['value'] = 0
        self.root.update_idletasks()

    def run_listed_scrape_in_thread(self, input_csv_path):
        """Runs the ListedGameScraper in the asyncio event loop via a thread."""
        try:
            self.status_label.config(text=f"Scraping from {os.path.basename(input_csv_path)}... Check console.")
            output_filename = f"search_results_{os.path.splitext(os.path.basename(input_csv_path))[0]}.csv"
            scraper = ListedGameScraper(input_csv_path, output_filename)
            success = asyncio.run(scraper.run_scrape())

            if success:
                self.status_label.config(text=f"Scraping finished. Check '{output_filename}'")
                messagebox.showinfo("Success", f"Listed games scrape completed! Results saved to '{output_filename}'.")
            else:
                self.status_label.config(text="Scraping failed. Check console for errors.")
                messagebox.showerror("Error", "Listed games scrape failed. See console for details.")

        except Exception as e:
            error_msg = f"Error during listed scrape: {e}"
            print(f"🚨 {error_msg}")
            self.status_label.config(text=error_msg)
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")
        finally:
            self.root.after(0, self._enable_buttons)

    def start_listed_scrape_thread(self):
        """Handles file selection and starts the listed scraping process in a thread."""
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("Busy", "Another process is already running.")
            return

        input_csv_path = filedialog.askopenfilename(
            title="Select Input CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )

        if not input_csv_path:
            self.input_file_label.config(text="Input CSV: Not selected")
            return

        self.input_file_label.config(text=f"Input CSV: {os.path.basename(input_csv_path)}")
        self._disable_buttons()
        self.status_label.config(text="Starting listed scrape thread...")

        self.active_thread = threading.Thread(
            target=self.run_listed_scrape_in_thread,
            args=(input_csv_path,),
            daemon=True
        )
        self.active_thread.start()

    def run_full_scrape_in_thread(self):
        """Runs the full async scrape function in the asyncio event loop via a thread."""
        try:
            self.status_label.config(text="Full scraping started... Check console/terminal.")
            asyncio.run(run_full_scrape())
            self.status_label.config(text="Full scraping finished. Check scraped_data.csv")
            messagebox.showinfo("Success", "Full Steam data scrape completed!")
        except Exception as e:
            error_msg = f"Error during full scrape: {e}"
            print(f"🚨 {error_msg}")
            self.status_label.config(text=error_msg)
            messagebox.showerror("Error", f"An error occurred during full scraping: {e}")
        finally:
            self.root.after(0, self._enable_buttons)

    def start_full_scrape_thread(self):
        """Starts the full scraping process in a separate thread."""
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("Busy", "Another process is already running.")
            return

        self._disable_buttons()
        self.status_label.config(text="Starting full scrape thread...")
        self.input_file_label.config(text="Input CSV: Not applicable")

        self.active_thread = threading.Thread(
            target=self.run_full_scrape_in_thread,
            daemon=True
        )
        self.active_thread.start()

    def run_db_process_in_thread(self):
        """Runs the async DatabaseHandler process in the asyncio event loop via a thread."""
        try:
            self.status_label.config(text="Database process started... Check console.")
            handler = DatabaseHandler()
            if not handler.config:
                raise ValueError("DatabaseHandler failed to initialize (config error).")

            success = asyncio.run(handler.run_db_process())

            if success:
                self.status_label.config(text="Database process finished successfully.")
                messagebox.showinfo("Success", "Database fetch, scrape, and store process completed!")
            else:
                self.status_label.config(text="Database process failed. Check console.")
                messagebox.showerror("Error", "Database fetch, scrape, and store process failed. See console for details.")

        except Exception as e:
            error_msg = f"Error during DB process: {e}"
            print(f"🚨 {error_msg}")
            self.status_label.config(text=error_msg)
            messagebox.showerror("Error", f"An unexpected error occurred during the DB process: {e}")
        finally:
            self.root.after(0, self._enable_buttons)

    def start_db_process_thread(self):
        """Starts the database fetch and store process in a thread."""
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("Busy", "Another process is already running.")
            return

        self._disable_buttons()
        self.status_label.config(text="Starting database process thread...")
        self.input_file_label.config(text="Input CSV: Not applicable")

        self.active_thread = threading.Thread(
            target=self.run_db_process_in_thread,
            daemon=True
        )
        self.active_thread.start()

    def run_db_insert_in_thread(self, input_csv_path):
        """Runs the database insertion process in a thread."""
        try:
            self.update_status(f"Starting DB insert for {os.path.basename(input_csv_path)}...")
            self.update_progress(0, 0)

            success, message = insert_csv_to_db(input_csv_path, self.update_status, self.update_progress)

            if success:
                self.update_status(f"DB Insert finished. {message}")
                messagebox.showinfo("Success", f"Database insert completed!\n{message}")
            else:
                self.update_status(f"DB Insert failed. {message}")
                messagebox.showerror("Error", f"Database insert failed.\n{message}")

        except Exception as e:
            error_msg = f"Unexpected error during DB insert: {e}"
            print(f"🚨 {error_msg}")
            self.update_status(error_msg)
            messagebox.showerror("Error", f"An unexpected error occurred during the DB insert process: {e}")
        finally:
            self.root.after(0, self._enable_buttons)

    def start_db_insert_thread(self):
        """Handles file selection and starts the DB insertion process in a thread."""
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("Busy", "Another process is already running.")
            return

        input_csv_path = filedialog.askopenfilename(
            title="Select CSV File to Insert into DB",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )

        if not input_csv_path:
            self.input_file_label.config(text="Input CSV: Not selected")
            return

        self.input_file_label.config(text=f"Input CSV: {os.path.basename(input_csv_path)}")
        self._disable_buttons()
        self.update_status("Starting database insert thread...")
        self.update_progress(0, 0)

        self.active_thread = threading.Thread(
            target=self.run_db_insert_in_thread,
            args=(input_csv_path,),
            daemon=True
        )
        self.active_thread.start()

def start_gui():
    """Initializes and runs the Tkinter GUI."""
    root = tk.Tk()
    app = AppGUI(root)
    root.mainloop()
