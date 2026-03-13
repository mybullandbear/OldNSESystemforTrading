import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def capture_charts(symbol):
    """
    Captures a screenshot of the charts for the given symbol from the local dashboard.
    Returns the path to the screenshot file.
    """
    screenshot_path = f"{symbol}_signal_capture.png"
    
    # Configure Headless Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--hide-scrollbars")
    
    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # Open Dashboard
        driver.get("http://127.0.0.1:5000")
        
        # Wait for Page Load
        time.sleep(2) 
        
        # Switch to Charts View
        # Find the "Charts" toggle button and click it
        charts_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-view='charts']"))
        )
        charts_btn.click()
        
        # Wait for Charts to Render (they might be hidden initially)
        time.sleep(1)
        
        # Identify the container to screenshot based on symbol
        # Create a mapping to find the specific row/div
        # Based on index.html:
        # NIFTY headers have "NIFTY 50", BANKNIFTY has "BANKNIFTY"
        
        target_indices = []
        if symbol == "NIFTY":
            # Finding the NIFTY section. It's the first chart-row usually.
            # We can select the parent container of the canvas
            target_element = driver.find_element(By.ID, "view-charts")
        elif symbol == "BANKNIFTY":
             target_element = driver.find_element(By.ID, "view-charts")
        else:
             target_element = driver.find_element(By.TAG_NAME, "body")
             
        # Take Screenshot
        target_element.screenshot(screenshot_path)
        print(f"Screenshot captured: {screenshot_path}")
        
        return os.path.abspath(screenshot_path)
        
    except Exception as e:
        print(f"Error capturing screenshot: {e}")
        return None
    finally:
        if driver:
            driver.quit()
