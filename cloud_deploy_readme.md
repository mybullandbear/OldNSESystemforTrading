# Deploying the NSE Option Chain Bot to Google Cloud VM

This package contains everything needed to run your trading system in the cloud, allowing it to stay online 24/7 without your local machine needing to stay on.

## 📦 Package Contents
1.  **Backend (`/`)**: Python Flask API that handles data fetching, database storage (`option_chain.db`), and trading logic.
2.  **Frontend (`/flutter_frontend`)**: The Flutter web dashboard code.
3.  **Docker Support**: `Dockerfile` and `docker-compose.yml` for easy isolated deployment.

## 🚀 Deployment Steps (On Your VM)

### 1. Transfer the Folder
Upload the `deploy_package` folder to your VM using SCP, SFTP, or by zipping it and downloading it via the VM terminal.

### 2. Install Requirements
We recommend using **Docker** for the easiest setup. If you have Docker installed on your VM:
```bash
docker-compose up --build -d
```
This will start both the backend and the database in the cloud.

**Manual Setup (Alternative):**
If you prefer running without Docker:
```bash
# Install Python dependencies
pip install -r requirements.txt

# Run the backend
python app.py
```

### 3. Change Frontend API Base (Crucial)
Before building the Flutter app, you must update the API URL to point to your VM's IP address.
*   Open `flutter_frontend/lib/services/api_service.dart`
*   Change line 6:
    ```dart
    static const String _base = 'http://YOUR_VM_IP:5000/api';
    ```

### 4. Build & Serve Flutter Web
```bash
cd flutter_frontend
flutter build web
# Then serve the 'build/web' folder using Nginx or Apache
```

## 🏦 Database Management
The `option_chain.db` (SQLite) will be created and maintained directly on the VM. It will automatically save all fetched data and trading history in the cloud.

## 🔐 Security Tip
On your Google Cloud Console, ensure you open **Port 5000** (Backend) and **Port 80** (Frontend) in the Firewall rules to allow access.
