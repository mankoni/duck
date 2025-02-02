import os
from zoneinfo import ZoneInfo
import datetime
import paramiko

def create_sftp_connection():
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => create_sftp_connection\n')

    hostname = 'sftp.adrive.com'  # e.g., 'sftp.example.com'
    port = 22  # Default SFTP port
    username = 'HughBarn@Hotmail.com'  # Your SFTP username
    password = 'n0Password'  # Your SFTP password

    try:
        # Set up the SSH client
        ssh = paramiko.SSHClient()

        # Automatically add the SFTP server's host key (optional for security)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the server
        print(f"Connecting to {hostname}...")
        ssh.connect(hostname, port=port, username=username, password=password)

        # Start the SFTP session, and print verification of remote directory
        sftp = ssh.open_sftp()
        print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] :Connected to {hostname}. \n")
        return sftp

    except Exception as e:
        print(f"Error connecting to {hostname}: {e}")
        return None


def upload_daily_run_log(sftp):

    local_log_path = "C:\\Users\\shanki\\Downloads\\DuckDataset\\Daily_Logs"
    remote_log_path = 'Logs'
    log_files = (os.listdir(local_log_path))
    log_file = max(log_files,key=lambda x:x.split('_',1)[1])
    remote_log_file_path = 'Logs/'+log_file
    local_log_file_path = os.path.join(local_log_path,log_file)
    print((log_file))
    sftp.put(local_log_file_path,remote_log_file_path)



sftp = create_sftp_connection()
upload_daily_run_log(sftp)