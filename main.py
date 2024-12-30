import os
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_backup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Constants from .env with error checking
required_env_vars = {
    "DB_CONTAINER_NAME": os.getenv("DB_CONTAINER_NAME"),
    "DB_NAME": os.getenv("DB_NAME"),
    "DB_USER": os.getenv("DB_USER"),
    "SUPABASE_URL": os.getenv("SUPABASE_URL"),
    "SUPABASE_KEY": os.getenv("SUPABASE_KEY"),
    "SUPABASE_BUCKET": os.getenv("SUPABASE_BUCKET"),
    "BACKUP_DIR": os.getenv("BACKUP_DIR", "backups"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD")
}

# Check for missing environment variables
missing_vars = [var for var, value in required_env_vars.items() if value is None]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Use the validated environment variables
DB_CONTAINER_NAME = required_env_vars["DB_CONTAINER_NAME"]
DB_NAME = required_env_vars["DB_NAME"]
DB_USER = required_env_vars["DB_USER"]
SUPABASE_URL = required_env_vars["SUPABASE_URL"]
SUPABASE_KEY = required_env_vars["SUPABASE_KEY"]
SUPABASE_BUCKET = required_env_vars["SUPABASE_BUCKET"]
BACKUP_DIR = required_env_vars["BACKUP_DIR"]
DB_PASSWORD = required_env_vars["DB_PASSWORD"]

# Ensure the backup directory exists
os.makedirs(BACKUP_DIR, exist_ok=True)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def backup_database():
    """Dump the database into a SQL file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"db_backup_{timestamp}.sql")

    try:
        cmd = [
            "docker", "exec",
            "-e", f"PGPASSWORD={DB_PASSWORD}",
            DB_CONTAINER_NAME,
            "pg_dump",
            "-U", DB_USER,
            "-h", "postgres",
            "-p", "5432",
            DB_NAME
        ]
        
        logger.info(f"Starting database backup")
        with open(backup_file, "w") as output_file:
            subprocess.run(
                cmd,
                stdout=output_file,
                check=True
            )
        
        logger.info(f"Database backup successful: {backup_file}")
        return backup_file
    except subprocess.CalledProcessError as e:
        logger.error(f"Error during database backup: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during backup: {str(e)}")
        return None

def upload_to_supabase(file_path):
    """Upload the backup file to Supabase Storage."""
    file_name = os.path.basename(file_path)

    try:
        logger.info(f"Starting upload of file: {file_name}")
        with open(file_path, "rb") as file_data:
            response = supabase.storage.from_(SUPABASE_BUCKET).upload(file_name, file_data)
            
        logger.info(f"File uploaded successfully to: {SUPABASE_BUCKET}/{file_name}")

    except Exception as e:
        logger.error(f"Error uploading to Supabase: {e}")
        if hasattr(e, 'message'):
            logger.error(f"Error message: {e.message}")

def cleanup_old_backups(days=7):
    """Delete local backup files older than 'days' days."""
    now = datetime.now()
    for filename in os.listdir(BACKUP_DIR):
        file_path = os.path.join(BACKUP_DIR, filename)
        if os.path.isfile(file_path):
            file_age = (now - datetime.fromtimestamp(os.path.getmtime(file_path))).days
            if file_age > days:
                os.remove(file_path)
                logger.info(f"Deleted old backup: {file_path}")

if __name__ == "__main__":
    logger.info("Starting backup process")
    backup_file = backup_database()
    if backup_file:
        upload_to_supabase(backup_file)
        cleanup_old_backups(days=7)
    logger.info("Backup process completed")