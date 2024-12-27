import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Constants from .env
DB_CONTAINER_NAME = os.getenv("DB_CONTAINER_NAME")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")
BACKUP_DIR = os.getenv("BACKUP_DIR")

# Ensure the backup directory exists
os.makedirs(BACKUP_DIR, exist_ok=True)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def backup_database():
    """Dump the database into a SQL file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"db_backup_{timestamp}.sql")

    try:
        # Add PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = os.getenv("DB_PASSWORD")  # Add this to your .env file
        
        subprocess.run(
            [
                "docker", "exec", DB_CONTAINER_NAME, 
                "pg_dump", 
                "-U", DB_USER,
                "-h", "localhost", 
                "-p", "5433", # Add explicit host
                DB_NAME
            ],
            stdout=open(backup_file, "w"),
            check=True,
            env=env
        )
        print(f"Database backup successful: {backup_file}")
        return backup_file
    except subprocess.CalledProcessError as e:
        print(f"Error during database backup: {e}")
        return None

def upload_to_supabase(file_path):
    """Upload the backup file to Supabase Storage."""
    file_name = os.path.basename(file_path)

    try:
        with open(file_path, "rb") as file_data:
            response = supabase.storage.from_(SUPABASE_BUCKET).upload(file_name, file_data)

        if response.get("error"):
            print(f"Error uploading to Supabase: {response['error']}")
        else:
            print(f"File uploaded to Supabase: {SUPABASE_BUCKET}/{file_name}")
    except Exception as e:
        print(f"Error uploading to Supabase: {e}")

def cleanup_old_backups(days=7):
    """Delete local backup files older than 'days' days."""
    now = datetime.now()
    for filename in os.listdir(BACKUP_DIR):
        file_path = os.path.join(BACKUP_DIR, filename)
        if os.path.isfile(file_path):
            file_age = (now - datetime.fromtimestamp(os.path.getmtime(file_path))).days
            if file_age > days:
                os.remove(file_path)
                print(f"Deleted old backup: {file_path}")

if __name__ == "__main__":
    backup_file = backup_database()
    if backup_file:
        upload_to_supabase(backup_file)
        cleanup_old_backups(days=7)
