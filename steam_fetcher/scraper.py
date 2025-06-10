# scraper.py
import os
import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async # Import stealth

EXPECTED_COLUMNS = 9   # 8 data columns + SteamOSResultStatus
MAX_ROWS = 200000
SCROLL_AMOUNT = 500
SCROLL_WAIT = 10000  # Increased wait time
LOGIN_WAIT = 120000 # Increased login/manual interaction wait to 120 seconds
BATCH_INTERVAL = 10
CSV_FILENAME = "scraped_data.csv" # Consider making this configurable or returning data

# Update column names to match the required order
column_names = [
    "Row Number",    # 1 (for tracking, not in your requested output but kept for uniqueness)
    "Last Change",   # 2
    "Title",         # 3
    "Developer",     # 4
    "Reviews",       # 5
    "Price",         # 6
    "Discount",      # 7
    "ProtonDB",      # 8
    "SteamOSResultStatus" # 9 (Derived from row class)
]

async def run_full_scrape():
    """
    Performs a full scrape of game data from checkmydeck.ofdgn.com.
    """
    start_row = 1
    if os.path.exists(CSV_FILENAME):
        try:
            df_existing = pd.read_csv(CSV_FILENAME)
            if not df_existing.empty and "Row Number" in df_existing.columns:
                # Ensure the column is numeric before calling max()
                numeric_rows = pd.to_numeric(df_existing["Row Number"], errors='coerce')
                numeric_rows = numeric_rows.dropna() # Remove non-numeric rows if any
                if not numeric_rows.empty:
                    start_row = int(numeric_rows.max()) + 1
            print(f"Resuming from row {start_row}")
        except pd.errors.EmptyDataError:
            print(f"CSV file '{CSV_FILENAME}' is empty. Starting from row 1.")
        except Exception as e:
            print(f"Error reading CSV for resume info: {e}. Starting from row 1.")

    unique_rows = {}
    processed_rows = 0
    no_new_rows_count = 0
    scroll_attempt = 0
    written_keys = set()
    max_row_seen = start_row - 1

    # --- Connect to existing Chrome instance --- 
    async with async_playwright() as p:
        try:
            print("🔌 Attempting to connect to Chrome over CDP (port 9222)...")
            # Connect to the manually launched Chrome instance
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            print("✅ Successfully connected to existing Chrome instance.")

            # Use the default context provided by the connected browser
            # Usually, you don't create a new context when connecting this way,
            # you use the one the browser already has.
            # Let's try getting the first existing context.
            if not browser.contexts:
                 print("⚠️ No existing browser contexts found. Creating a new one.")
                 # This might not be ideal, as it might not inherit the logged-in state/cookies
                 # from the tab where you solved Cloudflare. Consider using the existing context.
                 context = await browser.new_context(
                     user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                     viewport={'width': 1920, 'height': 1080},
                     device_scale_factor=1,
                     is_mobile=False,
                     has_touch=False,
                 )
            else:
                print("✅ Using the first existing browser context.")
                context = browser.contexts[0]
                # Ensure the context isn't closed unexpectedly
                context.on("close", lambda: print("Browser context closed unexpectedly."))

            # Get the page where you likely solved Cloudflare
            # Check if there are existing pages in the context
            if not context.pages:
                print("⚠️ No existing pages found in the context. Creating a new page.")
                page = await context.new_page()
                # Need to navigate again if we created a new page
                url = "https://checkmydeck.ofdgn.com/all-games?sort=deck-compat-last-change-desc"
                print(f"Navigating new page to {url}...")
                await page.goto(url, timeout=60000)
            else:
                # Try to use the *last* opened page, assuming it's the active one
                page = context.pages[-1]
                print(f"✅ Using existing page: {page.url}")
                # Verify if it's the correct URL, otherwise navigate
                target_url_base = "https://checkmydeck.ofdgn.com/all-games"
                if not page.url.startswith(target_url_base):
                    print(f"⚠️ Existing page URL ({page.url}) doesn't match target ({target_url_base}). Navigating...")
                    await page.goto(target_url_base + "?sort=deck-compat-last-change-desc", timeout=60000)
                else:
                    print("✅ Existing page URL seems correct.")

        except Exception as e:
            print(f"🚨 Failed to connect to Chrome over CDP: {e}")
            print("Ensure Chrome was launched with --remote-debugging-port=9222 and is running.")
            return # Exit if connection fails

        # --- Stealth and Init Script (Potentially less critical now, but keep for robustness) ---
        try:
            await stealth_async(page) # Apply stealth to the controlled page
            print("🛡️ Applied playwright-stealth patches to the controlled page.")
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            print("🛡️ Added init script to hide navigator.webdriver.")
        except Exception as stealth_error:
            print(f"⚠️ Error applying stealth/init script: {stealth_error}")

        # --- Wait for Grid (No long login wait needed as manual interaction is done) ---
        try:
            print("⏳ Waiting for grid container...")
            await page.wait_for_selector("div[role='grid']", timeout=30000) # Wait for the grid to appear
            print("✅ Grid container found. Proceeding with data extraction...")
        except Exception as e:
            # Try taking a screenshot for debugging
            try:
                await page.screenshot(path="debug_screenshot_grid_not_found.png")
                print("📸 Debug screenshot saved as debug_screenshot_grid_not_found.png")
            except Exception as screenshot_error:
                print(f"📸 Could not save screenshot: {screenshot_error}")
            await browser.close() # Close the *connection*, not the browser itself
            print("Disconnected from browser.")
            return

        async def scroll_table():
            """Scrolls the grid container down."""
            try:
                grid_container = await page.query_selector("div[role='grid']")
                if grid_container:
                    await grid_container.evaluate(f"node => node.scrollBy(0, {SCROLL_AMOUNT});")
                    print(f"📜 Scrolled down by {SCROLL_AMOUNT} pixels. Waiting {SCROLL_WAIT / 1000}s...")
                    await page.wait_for_timeout(SCROLL_WAIT)
                else:
                    print("🚨 Grid container not found during scroll attempt.")
                    return False # Indicate scroll failure
            except Exception as e:
                print(f"🚨 Error during scroll: {e}")
                return False # Indicate scroll failure
            return True # Indicate scroll success

        # --- Pre-scroll to find the starting row ---
        pre_scroll_attempt = 0
        MAX_PRE_SCROLL_ATTEMPTS = 100 # Limit pre-scrolling
        found_target_in_pre_scroll = False
        while pre_scroll_attempt < MAX_PRE_SCROLL_ATTEMPTS:
            rows = await page.query_selector_all("div[role='row']")
            target_found = False
            highest_row_index_on_page = -1
            for row in rows:
                row_index_str = await row.get_attribute("aria-rowindex")
                if row_index_str:
                    try:
                        row_index = int(row_index_str)
                        highest_row_index_on_page = max(highest_row_index_on_page, row_index)
                        if row_index >= start_row:
                            target_found = True
                            break # Found a row at or past the start row
                    except ValueError:
                        continue # Skip if row index is not an integer
            
            if target_found:
                print(f"✅ Found target rows (>= {start_row}) during pre-scroll. Starting extraction.")
                found_target_in_pre_scroll = True
                break
            else:
                pre_scroll_attempt += 1
                print(f"⏳ Pre-scroll attempt {pre_scroll_attempt}/{MAX_PRE_SCROLL_ATTEMPTS}: Highest row index seen: {highest_row_index_on_page}. Target ({start_row}) not found. Scrolling...")
                if not await scroll_table():
                    print("🚨 Scrolling failed during pre-scroll. Aborting.")
                    await browser.close()
                    return # Exit if scroll fails

        if not found_target_in_pre_scroll:
            print(f"🚨 Reached max pre-scroll attempts ({MAX_PRE_SCROLL_ATTEMPTS}) without finding target row {start_row}. Aborting.")
            await browser.close()
            return # Exit if target row not found after pre-scrolling

        # --- Main data extraction loop ---
        while processed_rows < MAX_ROWS:
            rows = await page.query_selector_all("div[role='row']")
            print(f"📊 Found {len(rows)} row elements on the page.")

            if not rows:
                print("⚠️ No rows found on the page. Waiting and trying again...")
                await page.wait_for_timeout(SCROLL_WAIT) # Wait longer if no rows found
                if not await scroll_table():
                     print("🚨 Scrolling failed after finding no rows. Aborting.")
                     break # Exit loop if scroll fails
                continue # Try fetching rows again

            new_valid_rows_in_batch = 0
            current_max_in_page = max_row_seen # Track max row index *seen* in this pass

            for row in rows:
                row_index_str = await row.get_attribute("aria-rowindex")
                if not row_index_str:
                    # print("⚠️ Skipping row: Missing 'aria-rowindex'.")
                    continue
                try:
                    row_index = int(row_index_str)
                except ValueError:
                    # print(f"⚠️ Skipping row: Invalid 'aria-rowindex' ('{row_index_str}').")
                    continue

                # Update the highest row index seen on this page load
                current_max_in_page = max(current_max_in_page, row_index)

                # Skip rows before the starting point or already processed
                if row_index < start_row or row_index in unique_rows or row_index in written_keys:
                    continue

                # Stop if we exceed the overall MAX_ROWS limit
                if len(unique_rows) + len(written_keys) >= MAX_ROWS:
                    print(f"🏁 Reached MAX_ROWS limit ({MAX_ROWS}). Stopping collection.")
                    processed_rows = MAX_ROWS # Ensure loop termination condition is met
                    break # Exit inner loop

                # Extract data from cells
                row_data = [row_index] # Start with Row Number
                cells = await row.query_selector_all("div[role='gridcell']")

                # Expecting 7 data cells + 1 status derived from class
                if len(cells) < 7:
                    # Only print warning for non-header rows with unexpected cell count
                    if row_index != 1:
                        print(f"⚠️ Skipping row {row_index}: Expected at least 7 cells, found {len(cells)}.")
                    continue # Skip header row (index 1) or rows with too few cells

                # Extract text from the first 7 cells
                for i in range(7):
                    try:
                        text = (await cells[i].inner_text()).strip()
                    except Exception as cell_error:
                        print(f"⚠️ Error extracting text from cell {i+1} in row {row_index}: {cell_error}. Using empty string.")
                        text = ""
                    row_data.append(text)

                # Extract SteamOSResultStatus from the row's class attribute
                steam_os_status = "Unknown" # Default status
                try:
                    class_attr = await row.get_attribute("class") or ""
                    classes = class_attr.split()

                    # Check for specific status classes directly
                    if "verified" in classes:
                        steam_os_status = "Verified"
                    elif "playable" in classes:
                        steam_os_status = "Playable"
                    elif "unsupported" in classes:
                        steam_os_status = "Unsupported"
                    # Keep "Unknown" if none of the above are found

                except Exception as class_error:
                    print(f"⚠️ Error extracting SteamOSResultStatus from class in row {row_index}: {class_error}")
                row_data.append(steam_os_status) # Append the derived status

                # Final check for expected column count
                if len(row_data) != EXPECTED_COLUMNS:
                    print(f"⚠️ Skipping row {row_index}: Data mismatch. Expected {EXPECTED_COLUMNS} columns, got {len(row_data)}. Data: {row_data}")
                    continue

                unique_rows[row_index] = row_data
                processed_rows += 1
                new_valid_rows_in_batch += 1
                # print(f"✅ Added row {row_index}. Total unique rows collected this session: {len(unique_rows)}")

            # --- End of processing rows on current page ---

            if processed_rows >= MAX_ROWS:
                 break # Exit outer loop if max rows reached

            print(f"Batch summary: Processed {new_valid_rows_in_batch} new valid rows from this page view.")

            # Check if we are making progress
            if current_max_in_page > max_row_seen:
                max_row_seen = current_max_in_page
                no_new_rows_count = 0 # Reset counter because we saw higher row indices
                print(f"📈 Progress: Highest row index seen so far: {max_row_seen}")
            else:
                # Only increment no_new_rows_count if we didn't add any *new* rows *and* the max row index didn't increase
                if new_valid_rows_in_batch == 0:
                    no_new_rows_count += 1
                    print(f"⏳ No new rows added and max row index ({max_row_seen}) did not increase. Stall count: {no_new_rows_count}/3.")
                else:
                     # We added rows, but max didn't increase (maybe filled gaps). Reset counter.
                     no_new_rows_count = 0
                     print(f"📊 Added {new_valid_rows_in_batch} rows, but max row index ({max_row_seen}) unchanged.")


            if no_new_rows_count >= 3:
                print("🚨 No new rows detected after 3 consecutive attempts where max row index didn't increase. Assuming end of data.")
                break # Exit outer loop

            # Write batch to CSV periodically
            scroll_attempt += 1
            if scroll_attempt % BATCH_INTERVAL == 0 and unique_rows:
                # Get keys for rows collected but not yet written
                keys_to_write = sorted([key for key in unique_rows if key not in written_keys])
                if keys_to_write:
                    batch_data = [unique_rows[k] for k in keys_to_write]
                    df_batch = pd.DataFrame(batch_data, columns=column_names)
                    is_new_file = not os.path.exists(CSV_FILENAME) or os.path.getsize(CSV_FILENAME) == 0
                    try:
                        df_batch.to_csv(CSV_FILENAME, mode='a', header=is_new_file, index=False, encoding='utf-8')
                        written_keys.update(keys_to_write)
                        # Clear rows from memory after writing to save resources
                        for key in keys_to_write:
                            del unique_rows[key]
                        print(f"💾 Batch written to {CSV_FILENAME} with {len(batch_data)} rows. Total rows written this session: {len(written_keys)}. Cleared from memory.")
                    except Exception as write_error:
                         print(f"🚨 Error writing batch to CSV: {write_error}")
                else:
                    print("💾 No new rows collected since last batch write.")


            # Scroll for the next batch
            print(f"🌀 Scroll attempt {scroll_attempt}. Trying to scroll...")
            if not await scroll_table():
                 print("🚨 Scrolling failed. Ending data extraction.")
                 break # Exit outer loop

        # --- End of main extraction loop ---

        # Write any remaining collected rows
        # Use a different variable name to avoid potential conflicts if 'k' was used elsewhere
        remaining_keys_final = sorted([key for key in unique_rows if key not in written_keys])
        if remaining_keys_final:
            # Use the correct variable 'remaining_keys_final' here
            remaining_data = [unique_rows[key] for key in remaining_keys_final]
            df_remaining = pd.DataFrame(remaining_data, columns=column_names)
            is_new_file = not os.path.exists(CSV_FILENAME) or os.path.getsize(CSV_FILENAME) == 0
            try:
                df_remaining.to_csv(CSV_FILENAME, mode='a', header=is_new_file, index=False, encoding='utf-8')
                # Update written_keys with the keys from the final batch
                written_keys.update(remaining_keys_final)
                print(f"💾 Final batch written to {CSV_FILENAME} with {len(remaining_data)} rows.")
            except Exception as write_error:
                 print(f"🚨 Error writing final batch to CSV: {write_error}")

        total_written = len(written_keys)
        print(f"✅ Data extraction complete. Total rows written in this session: {total_written}. Data saved in '{CSV_FILENAME}'.")

        # --- Cleanup --- 
        # When done, just close the connection, don't close the browser window
        await browser.close()
        print("🔒 Disconnected from browser. Manual Chrome window remains open.")

# Example of how to run this if needed directly (e.g., for testing)
# You would typically call run_full_scrape() from your GUI handler.
# if __name__ == "__main__":
#     asyncio.run(run_full_scrape())
