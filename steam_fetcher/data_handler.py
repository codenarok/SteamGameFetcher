# steam_fetcher/db_handler.py
import configparser
import pyodbc
import pymongo
from pymongo import UpdateOne
import os
from azure.identity import DefaultAzureCredential, DeviceCodeCredential
import struct
import asyncio
from playwright.async_api import async_playwright

# --- Configuration ---
CONFIG_FILE = 'config.ini'

class DatabaseHandler:
    """
    Handles fetching game details from SQL Server using Managed Identity,
    scraping SteamOS compatibility for each title, and writing the
    specified fields plus the scraped result to MongoDB.
    """

    def __init__(self):
        self.config = self._read_config()
        if not self.config:
            raise ValueError("Failed to read or parse configuration file.")
        # Constants from config for scraping
        self.login_wait = 60000
        self.search_wait = 3000
        self.web_title_cell_index = 1 # Index of Title cell in web grid

    def _read_config(self):
        """Reads connection details and query from config.ini."""
        if not os.path.exists(CONFIG_FILE):
            print(f"🚨 Error: Configuration file '{CONFIG_FILE}' not found.")
            return None
        try:
            config = configparser.ConfigParser()
            config.read(CONFIG_FILE)
            # Basic validation
            if 'SQLServer' not in config or 'MongoDB' not in config:
                print("🚨 Error: Config file must contain [SQLServer] and [MongoDB] sections.")
                return None
            if not all(k in config['SQLServer'] for k in ['server', 'database', 'query']):
                 print("🚨 Error: [SQLServer] section missing server, database, or query.")
                 return None
            if not all(k in config['MongoDB'] for k in ['uri', 'database', 'collection']):
                 print("🚨 Error: [MongoDB] section missing uri, database, or collection.")
                 return None
            print("✅ Configuration loaded successfully.")
            return config
        except configparser.Error as e:
            print(f"🚨 Error reading configuration file: {e}")
            return None

    def _get_sql_connection(self):
        """
        Establishes a connection to SQL Server using Azure AD credentials
        obtained via Device Code Flow (interactive console login).
        """
        if not self.config: return None

        server = self.config['SQLServer']['server']
        database = self.config['SQLServer']['database']
        driver = '{ODBC Driver 17 for SQL Server}'

        conn_str = (
            f"Driver={driver};"
            f"Server={server};"
            f"Database={database};"
        )
        print(f"Attempting SQL connection to {server}/{database} using Azure AD Device Code Flow...")

        try:
            # --- Get Token using Device Code Flow ---
            # This will print instructions to the console for interactive login
            print("\nInitiating Azure AD Device Code Login...")
            print("Please follow the instructions printed below in your console:")
            credential = DeviceCodeCredential()

            # The resource URI for Azure SQL Database
            sql_resource_uri = "https://database.windows.net/.default"

            # Calling get_token() with DeviceCodeCredential triggers the interactive flow
            # if authentication is required. It will block until authentication completes or fails.
            token_object = credential.get_token(sql_resource_uri)
            print(f"\nSuccessfully obtained Azure AD token via Device Code Flow (expires {token_object.expires_on}).")

            token_bytes = token_object.token.encode("utf-16le")
            token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            attrs_before = {1256: token_struct}

            # Connect using the token
            conn = pyodbc.connect(conn_str, attrs_before=attrs_before, autocommit=True)

            print("✅ SQL Server connection successful using Azure AD token.")
            return conn

        except Exception as credential_error:
             # Catch errors from DeviceCodeCredential or get_token()
             print(f"\n🚨 Failed to obtain Azure AD token via Device Code Flow: {credential_error}")
             print("  Ensure network connectivity and that device code flow is permitted in your environment.")
             return None
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"🚨 SQL Server Connection Error: SQLSTATE {sqlstate} - {ex}")
            if 'Login failed for user' in str(ex):
                 print("  Checklist: ")
                 print("  1. Does your Azure AD user account have login permissions on the Azure SQL Server?")
                 print("  2. Has your Azure AD user been added as a user to the specific database ('{database}')?")
                 print("  3. Does the database user have appropriate permissions (e.g., db_datareader)?")
            return None
        except Exception as e:
             print(f"🚨 Unexpected error connecting to SQL Server: {e}")
             return None

    def fetch_game_details_from_sql(self):
        """
        Fetches game details from SQL Server based on the query in config.
        Returns a list of dictionaries containing all fetched columns.
        """
        if not self.config: return None

        sql_query = self.config['SQLServer']['query']
        print(f"Executing SQL query: {sql_query[:100]}...") # Log start of query

        conn = self._get_sql_connection()
        if not conn:
            return None

        game_details_list = []
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            # Get column names from cursor description
            columns = [column[0] for column in cursor.description]
            print(f"Fetched columns from SQL: {columns}") # Log fetched columns

            # Ensure TitleName is present, as it's needed for scraping
            required_for_scraping = 'TitleName'
            if required_for_scraping not in columns:
                 print(f"🚨 Error: SQL query result is missing required column for scraping: '{required_for_scraping}'. Check config.ini query.")
                 return None

            rows = cursor.fetchall()
            for row in rows:
                # Create a dictionary for each row using column names as keys
                game_details = dict(zip(columns, row))
                game_details_list.append(game_details)

            print(f"✅ Fetched {len(game_details_list)} records from SQL Server.")
        except pyodbc.Error as ex:
            print(f"🚨 SQL Query Error: {ex}")
            return None
        except Exception as e:
             print(f"🚨 Unexpected error during SQL query execution: {e}")
             return None
        finally:
            if conn:
                conn.close()
                print("SQL connection closed.")

        return game_details_list

    async def _get_steamos_result(self, page, search_box, title_name):
        """
        Searches checkmydeck for the title and returns the SteamOS status
        for an exact (case-insensitive) match. Returns status string or None.
        """
        print(f"  -> Scraping for: '{title_name}'")
        found_status = None
        if not title_name: # Handle empty title names
             print("  ⚠️ Skipping scrape due to empty title name.")
             return "Skipped (Empty)"
        try:
            await search_box.fill("")
            await search_box.fill(title_name)
            await page.wait_for_timeout(self.search_wait)

            grid_selector = "div[role='grid']"
            rows = await page.query_selector_all(f"{grid_selector} div[role='row']")

            if not rows:
                print(f"  ❌ No rows found in grid after searching.")
                return None # Indicate not found

            for row in rows:
                cells = await row.query_selector_all("div[role='gridcell']")
                if len(cells) > self.web_title_cell_index:
                    try:
                        web_title = (await cells[self.web_title_cell_index].inner_text()).strip()
                        if web_title.lower() == title_name.lower():
                            print(f"  ✅ Found exact match: '{web_title}'")
                            status = "N/A"
                            try:
                                class_attr = await row.get_attribute("class") or ""
                                classes = class_attr.split()
                                potential_status = classes[-1] if classes else ""
                                if potential_status.startswith("status-"):
                                     status = potential_status.replace("status-", "").capitalize()
                                elif len(classes) > 1:
                                    status = classes[-1]
                            except Exception as class_error:
                                print(f"  ⚠️ Error extracting status: {class_error}")
                            found_status = status
                            break # Stop after finding exact match
                    except Exception as cell_error:
                        print(f"  ⚠️ Error reading cell: {cell_error}")
                        continue
            if found_status is None:
                print(f"  ❌ No exact match found in filtered results.")
            return found_status # Will be None if no exact match found

        except Exception as e:
            print(f"  🚨 An error occurred during scraping for '{title_name}': {e}")
            return None # Indicate error/not found

    def write_results_to_mongo(self, results_list):
        """
        Writes the list of result dictionaries to MongoDB, avoiding exact duplicates
        using upsert operations.
        """
        if not self.config: return False
        if not results_list:
            print("ℹ️ No results provided to write to MongoDB.")
            return True

        mongo_uri = self.config['MongoDB']['uri']
        mongo_db_name = self.config['MongoDB']['database']
        mongo_collection_name = self.config['MongoDB']['collection']

        print(f"\nAttempting MongoDB connection to {mongo_db_name}/{mongo_collection_name}...")

        client = None # Initialize client to None
        try:
            client = pymongo.MongoClient(mongo_uri)
            client.admin.command('ismaster') # Verify connection
            print("✅ MongoDB connection successful.")

            db = client[mongo_db_name]
            collection = db[mongo_collection_name]

            # --- Upsert Logic ---
            print(f"Processing {len(results_list)} documents for upsert into MongoDB...")
            upserted_count = 0
            matched_count = 0

            # Option 1: Individual Upserts (Simpler to understand)
            for doc in results_list:
                # The filter matches *all* fields in the document to check for an exact duplicate
                query_filter = doc.copy() # Use a copy to avoid modifying the original dict if needed

                # Perform an upsert: update if found (based on filter), insert if not found.
                # Using $setOnInsert ensures we only write the data during insertion.
                # If a document with these exact values exists, matched_count increases.
                # If no exact match exists, upserted_count increases.
                result = collection.update_one(
                    filter=query_filter,
                    update={"$setOnInsert": doc},
                    upsert=True
                )
                if result.upserted_id:
                    upserted_count += 1
                elif result.matched_count > 0:
                    matched_count += 1

            print(f"✅ MongoDB upsert complete. Inserted: {upserted_count}, Matched existing: {matched_count}.")
            return True

        except pymongo.errors.ConnectionFailure as e:
            print(f"🚨 MongoDB Connection Error: {e}")
            return False
        except pymongo.errors.OperationFailure as e:
             print(f"🚨 MongoDB Operation Error: {e}")
             return False
        except Exception as e:
            print(f"🚨 Unexpected error interacting with MongoDB: {e}")
            return False
        finally:
            if client: # Check if client was successfully created before closing
                client.close()
                print("MongoDB connection closed.")

    async def run_db_process(self):
        """
        Orchestrates fetching from SQL, scraping SteamOS status,
        and writing specified results to MongoDB. (Async)
        """
        print("\n--- Starting Database Fetch, Scrape, and Store Process ---")
        fetched_game_details = self.fetch_game_details_from_sql() # Contains all SQL columns
        if fetched_game_details is None:
            print("🚨 Failed to fetch details from SQL Server. Aborting.")
            return False

        if not fetched_game_details:
             print("ℹ️ No game details fetched from SQL Server. Nothing to process.")
             return True

        # --- Initialize Playwright ---
        print("\nInitializing browser for scraping...")
        browser = None # Initialize browser variable
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False) # Or True if preferred
                context = await browser.new_context()
                page = await context.new_page()
                url = "https://checkmydeck.ofdgn.com/all-games?sort=deck-compat-last-change-desc"
                search_box_element = None
                try:
                    await page.goto(url, timeout=60000)
                    print(f"🚀 Browser opened. Please log in if required. Waiting {self.login_wait / 1000}s...")
                    await page.wait_for_timeout(self.login_wait)
                    search_input_selector = "input[placeholder='Type to filter']"
                    await page.wait_for_selector("div[role='grid']", timeout=30000)
                    search_box_element = await page.wait_for_selector(search_input_selector, timeout=30000)
                    print("✅ Browser ready for scraping.")
                except Exception as e:
                    print(f"🚨 Browser initialization/navigation failed: {e}")
                    return False # Cannot proceed without browser

                if not search_box_element:
                     print("🚨 Could not find search box element on page.")
                     return False

                # --- Scrape status and prepare final documents ---
                print(f"\nScraping SteamOS status for {len(fetched_game_details)} titles...")
                results_for_mongo = []
                required_mongo_fields = ['TitleName', 'TitleID', 'PublisherName', 'ProductID', 'PublisherType']

                for index, game_data in enumerate(fetched_game_details):
                    title_to_scrape = game_data.get('TitleName', '').strip()
                    print(f"Processing {index + 1}/{len(fetched_game_details)}: '{title_to_scrape}'")

                    status = await self._get_steamos_result(page, search_box_element, title_to_scrape)
                    scraped_status = status if status is not None else "Not Found"

                    # --- Construct the final document for MongoDB ---
                    final_doc = {}
                    all_keys_present = True
                    for key in required_mongo_fields:
                        if key in game_data:
                            final_doc[key] = game_data[key]
                        else:
                            print(f"⚠️ Warning: Key '{key}' not found in fetched data for title '{title_to_scrape}'. Setting to None.")
                            final_doc[key] = None
                            all_keys_present = False

                    final_doc['SteamOSResult'] = scraped_status

                    if not all_keys_present:
                         print(f"  -> Document for '{title_to_scrape}' has missing fields.")

                    results_for_mongo.append(final_doc)

        except Exception as e:
             print(f"🚨 An unexpected error occurred during the Playwright/Scraping phase: {e}")
             return False
        finally:
            if browser:
                await browser.close()
                print("\n🔒 Browser closed.")

        if not results_for_mongo:
             print("ℹ️ No results were prepared for MongoDB.")
             success = False
        else:
            # Call the updated write method
            success = self.write_results_to_mongo(results_for_mongo)

        if success:
            print("\n--- Database Fetch, Scrape, and Store Process Completed Successfully ---")
        else:
            print("\n🚨 Database Fetch, Scrape, and Store Process Failed ---")
        return success
