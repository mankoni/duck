from zoneinfo import ZoneInfo
import datetime
import google.generativeai as genai
import asyncio
from PIL import Image
import os
import time
import json
import shutil
import paramiko
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

"""
                                        OVERVIEW OF THE PROCESS FLOW

                                        NOTE: STEPS 1 - 10 are performed in a 
                                        loop for each CAM a.k.a remote_dir_root

1.  Step 1 is to create a SFTP connection with Remote Host (ADrive). 
    This is done by function "def create_sftp_connection() ; return sftp" 
    which returns sftp object that will be used in subsequent steps to connect to ADrive. The connections parameters are local to function.

2.  Step 2 is to get the control data which has key information about the last processed folder for each Camera(corresponds to root directory in ADrive).
    THis is done by function call "def get_ctrl_json(sftp , remote_ctrl_path) ; return json_data" 
    which returns the ctrl data from the ADrive in a JSON format. 
    There are 2 additional keys in this ctrl data with a -OVR suffix used to manually enter values for folder(s) that can be processed overriding the normal run.

3.  Step 3 is to check for Override folders. 
    This is done by function call "def check_override_folders(json_ctrl_data ,remote_dir_roots) ; return override_run (boolean)"
    which returns a boolean flag indicating if there is an override for one or more of the CAMS. We iterate thru all the Override keys
    in json_ctrl_data and set the flag to True if at least one override request is present
    We want to check this first as a presence of folder(s) will short circuit the normal flow.

4.  Step 4 is to get a list of ALL the folders under a remote root. 
    This is done by calling function "def get_remote_folders_list(sftp , remote_dir_root) ;  return  remote_folders"
    which returns the list of remote folders (Ex: for Remote Root SSAK-245923-BAAEC-CAM1, the returned list will contain folder names like [20250108, 20250109, 20250110, ....] )

5.  Step 5 is to derive the folders that needs to be processed for the current execution of this script. 
    This is done by calling function "def derive_remote_folders_to_process(last_processed_folder , remote_folders) ; return folders_to_process[:-1:]" 
    which returns a list of folders to be processed EXCLUDING THE MOST RECENT FOLDER ([:-1:] slice operator excludes last element of array:
    Logic to derive this get all the folders from the previous step and compare it to last processed folder from step 2.
    Filter the folders > last processed folder; exclude the last element and return the list.
    We exclude the last/recent element as EST run time may still be in the middle of uploads for UK time. So to play it safe
    we will INTENTIONALLY lag a day in our processing of folders.

6.  Step 6 is to rename the folders in format YYYYMMDD_000n. 
    This is done by calling function "def rename_remote_folders(folders_to_process, remote_dir_root,sftp)"
    This is done for convenience as the camera uploads the image filenames in timestamp manner. We want to 
    make it easy to identify the image file names that are named in numerical order.
    NOTE: The renaming of the files are done ON THE SERVER.

7.  Step 7 is to download the folder(s) images to local drive.
    This is done by calling function "def download_images_to_local(folders_to_process , local_dir_root, remote_dir_root, raw_image_count, sftp) ; return raw_image_count
    which returns the count(total number of images in each folder that was downloaded) used for reporting. NO bearing on remaining logic
    We download the images to a local folder called /some_local_root /{folder name}/Images via sftp(get). This is done for each folder determined in Step 5.
    We also create 2 additional empty folders , "fnd" and "ntf" under the same local root for storing the matched images and unmatched images
    THe existence of all 3 folders are checked first and it they exist, they are deleted and recreated. This makes the whole process idempotent.

8.  Step 8 is to call Gemini Flash API to analyze each image.
    This is done by calling function "async def process_images(folders_to_process , local_dir_root,remote_dir_root, gemini_error_count) ; return gemini_error_count"
    Function iterates thru each folder to be processed and for each folder iterates thru images in the folder.
    Note that the folders/Images  are now on local PC because of Step7.
    Based on Gemini response, the Images are COPIED (not moved) from the some_local_root/Images folder to
    either "fnd" or "ntf" folder.
    At the end of this function, the sum of image files in "fnd" and "ntf" folders should = files in "Images" folder
    unless there was a quota error which resulted in some files being skipped
    IN this step, we also capture Gemini response for each image file and write it to a text file stored
    in the "fnd" folder. We chose this folder as in the next step, the content of the "fnd" folder gets
    copied to the remote. Hence the Gemini responses are available in the remote server for analysis.
    At this time Gemini is rate limiting to 15 Tx/ min. So we introduce a sleep time of 3 sec assuming 1 sec
    processing time.
    An array containing error counts during processing is returned for reporting purposes

9.  Step 9 is to copy the content of the "fnd" folder to the remote server.
    This is done by calling function "def copy_filtered_images_local_to_remote(folders_to_process , local_dir_root,remote_dir_root,sftp, filtered_image_count)"
    ;  return (filtered_image_count)
    We create a folder on the remote server called 'FILTERED IMAGES" and copy the files in the "fnd" folder via sftp(put). We now have 
    successfully copied the images that matches our criteria(prompt in Gemini Flash call in Step 8)
    A count of files copied is returned for reporting purposes

10. Step 10 is to update the control data.
    This is done by calling function "def update_ctrl_data(folders_to_process , json_ctrl_data , remote_dir_root , override_run , sftp)"
    Now that we have processed ALL folders for a given CAM and copied them to ADrive, our job is done and we can now update the
    control data with the last folder that was processed for the given CAM.
    We take the max(folders_to_process)to get the latest folder name and then call sftp(put) to rewrite the control data on the ADrive.

11. Step 11 is to generate the report.
    This is done by calling def generate_report(start_time,raw_image_count,filtered_image_count,override_run); return report
    The report is constructed by f'' approach line by line 
    The function returns the entire report in a string variable

12. Step 12 is to email the report.
    This is done by calling def send_email(report ,  override_run)
    Uses Gmail to send report. Password for gmail is stored in Local PC as a Environment variable


"""


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


def get_ctrl_json(sftp, remote_ctrl_path):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => get_ctrl_json\n')
    sftp.get(remote_ctrl_path, 'ctrl_data3.json')
    with open('ctrl_data3.json', 'r') as f:
        json_data = json.load(f)
        print(f'JSON Control data {json_data}\n')
        return json_data


def check_override_folders(json_ctrl_data, remote_dir_roots):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => check_override_folders\n')

    override_run = False
    for remote_dir_root in remote_dir_roots:
        override_key = remote_dir_root + "-OVR"
        override_folders = json_ctrl_data[override_key]
        if override_folders:
            override_run = True
    return override_run


def get_remote_folders_list(sftp, remote_dir_root):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => get_remote_folders_list\n')
    remote_folders = sftp.listdir(remote_dir_root)
    remote_folders.sort()
    return remote_folders


def derive_remote_folders_to_process(last_processed_folder, remote_folders):
    ### We want to exclude the MOST recent folder i.e the LAST one in the list  ###
    ### Since we run in USA Time, File upload happens in the evening EST and takes a while to complete ###
    ### Hence to avoid processing partial day's images, we INTENTIONALLY are a DAY BEHIND to play it safe ###

    print(
        f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => derive_remote_folders_to_process\n')
    folders_to_process = [folder for folder in remote_folders if folder > last_processed_folder]
    folders_to_process.sort()  # sorting ensures we get the most recent folder as the last element
    return folders_to_process[:-1:]  ### SUPER IMPORTANT. Return all folders except last one ###


def rename_remote_folders(folders_to_process, remote_dir_root, sftp):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => rename_remote_folders\n')
    for folder in folders_to_process:
        remote_dir = remote_dir_root + "/" + folder + "/images"
        print(f"Remote directory {remote_dir}  ==> ", end=' ')
        remote_files = sftp.listdir(remote_dir)
        print("Item count ", len(remote_files))

        if remote_files:
            files_with_ts = []
            for imagefile in remote_files:
                remotefile = remote_dir + "/" + imagefile
                file_stat = sftp.stat(remotefile)
                modified_time = file_stat.st_mtime
                files_with_ts.append((remotefile, file_stat.st_mtime))

            files_with_ts.sort(key=lambda x: x[1])
            for idx, (remotefile, timestamp) in enumerate(files_with_ts, start=1):

                newfilename = remote_dir + "/" + folder + "_" + str(idx).zfill(4) + ".jpg"

                if "_" in remotefile:
                    print(f"Skipping renaming of file {remotefile}  - Already in proper format")
                else:
                    print(f"orig file  {remotefile} rename to ==> {newfilename}")
                    sftp.rename(remotefile, newfilename)
        else:
            print("No files found in the remote directory.", remote_dir)


def download_images_to_local(folders_to_process, local_dir_root, remote_dir_root, raw_image_count, sftp):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => download_images_to_local\n')
    for folder in folders_to_process:
        remote_dir = remote_dir_root + "/" + folder + "/images"
        print(f'REMOTE DIR = {remote_dir}')
        local_dir = os.path.join(local_dir_root, remote_dir_root, folder, "Images")
        fnd_dir = os.path.join(local_dir_root, remote_dir_root, folder, 'fnd')
        ntf_dir = os.path.join(local_dir_root, remote_dir_root, folder, "ntf")

        if os.path.exists(local_dir):

            # If the directory is not empty, use shutil.rmtree
            try:
                shutil.rmtree(local_dir)  # Delete the directory and its contents

            except Exception as e:
                print(f"Error deleting directory {local_dir}: {e}")
        os.makedirs(local_dir)

        if os.path.exists(fnd_dir):

            # If the directory is not empty, use shutil.rmtree
            try:
                shutil.rmtree(fnd_dir)  # Delete the directory and its contents

            except Exception as e:
                print(f"Error deleting directory {fnd_dir}: {e}")

        os.makedirs(fnd_dir)

        if os.path.exists(ntf_dir):

            # If the directory is not empty, use shutil.rmtree
            try:
                shutil.rmtree(ntf_dir)  # Delete the directory and its contents

            except Exception as e:
                print(f"Error deleting directory {ntf_dir}: {e}")

        os.makedirs(ntf_dir)

        remote_files = sftp.listdir(remote_dir)

        image_count = (len(remote_files))
        raw_image_count.append((remote_dir_root, folder, image_count))

        print(f"Downloading  {len(remote_files)}  Images from remote dir {remote_dir}\n")
        # raw_image_count.append(len(remote_files))
        remote_files.sort()
        if remote_files:
            # print("Processing files in remote Dir --> ", remote_dir)
            for imagefile in remote_files:
                # print(imagefile)
                remotefile = remote_dir + "/" + imagefile
                localfile = local_dir + "/" + imagefile
                sftp.get(remotefile, localfile)
                print(f"Local get of {remotefile} success for {localfile}")

        else:
            print("No files found in the remote directory.", remote_dir)

    sftp.close()
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}] : SFTP CLOSED after remote copied to Local\n')
    return raw_image_count


async def process_images(folders_to_process, local_dir_root, remote_dir_root, gemini_error_count):
    print(
        f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => process_images(Calling Gemini API)\n')

    genai.configure(api_key="AIzaSyCkMVDojZ82ciCBo2VqSlBFe2ebU-74KdM")
    model = genai.GenerativeModel("gemini-1.5-flash")
    gemini_responses = []
    error_count = 0

    for process_folder in folders_to_process:
        folder_path = local_dir_root + "/" + remote_dir_root + "/" + process_folder + "/" + "Images"
        fnd_root = os.path.join(local_dir_root, remote_dir_root, process_folder, "fnd")
        ntf_root = os.path.join(local_dir_root, remote_dir_root, process_folder, "ntf")

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            fnd_path = os.path.join(fnd_root, filename)
            ntf_path = os.path.join(ntf_root, filename)
            # print(f"Processing: {file_path}")

            try:
                # Open the image
                img = Image.open(file_path)

                # Send request to the model
                response = model.generate_content(["Is there a shelduck in this image?", img])

                # If response.resolve() is asynchronous, await it
                if hasattr(response, 'resolve') and callable(getattr(response, 'resolve')):
                    # Check if 'resolve' is a coroutine and await it
                    if asyncio.iscoroutinefunction(response.resolve):
                        # print('co routine')
                        await response.resolve()
                    else:
                        # print('not co routine')
                        response.resolve()  # If it's not a coroutine, just call it directly
                else:
                    print("No resolve method found on response.")

                print(f"Response for file : {filename} : ==> : {response.text}")
                # if response.text == "NO":
                if "No" in response.text:
                    gemini_responses.append(f'Duck not found | {filename} | No | {response.text}')
                    img.close()

                    try:
                        shutil.copy(file_path, ntf_path)

                    except FileNotFoundError:
                        print(f"The file {file_path} copy to ntf folder failed")

                elif "Yes" in response.text:
                    gemini_responses.append(f'Duck found | {filename} | Yes| {response.text}')
                    img.close()
                    try:
                        shutil.copy(file_path, fnd_path)

                    except FileNotFoundError:
                        print(f"The file {file_path} copy to fnd folder failed")

                else:
                    gemini_responses.append(f'Cannot Determine | {filename} | N/A | {response.text}')
                    img.close()
                    try:
                        shutil.copy(file_path, ntf_path)

                    except FileNotFoundError:
                        print(f"The file {file_path} copy to ntf folder failed")

                # sleep for 3 sec since Google API has a limit of 15 req/min
                time.sleep(3)
            except Exception as e:
                error_count += 1
                # Catch any errors during the response generation and processing
                print(f"Error processing {filename}: {e}")
        gemini_error_count.append((remote_dir_root, process_folder, error_count))

        gemini_file_name = f'gemini_responses_{process_folder}.txt'

        gemini_file_path = os.path.join(fnd_root, gemini_file_name)

        with open(gemini_file_path, 'w') as f:
            for item in gemini_responses:
                f.write(str(item) + "\n")

    return gemini_error_count


def copy_filtered_images_local_to_remote(folders_to_process, local_dir_root, remote_dir_root, sftp,
                                         filtered_image_count):
    print(
        f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => copy_filtered_images_local_to_remote\n')

    for process_folder in folders_to_process:

        ##  Create the remote directories to be copied ###
        remote_dir_path = remote_dir_root + "/" + process_folder + "/" + "FILTERED IMAGES"

        try:
            sftp.stat(remote_dir_path)

        except  FileNotFoundError:
            print(f"\nCreating Remote dir {remote_dir_path}")
            try:
                sftp.mkdir(remote_dir_path)
            except Exception as e:
                print(f"Error creating directory {remote_dir_path}: {e}")

        ## End of Remote Directories creation  ###

        fnd_root = os.path.join(local_dir_root, remote_dir_root, process_folder, "fnd")

        image_count = len(list(os.listdir(
            fnd_root))) - 1  # subtract 1 from the number of files in the fnd directory to exclude the Gemini Responses file
        filtered_image_count.append((remote_dir_root, process_folder,
                                     image_count))  # Is a tuple consisting of (cam1/2, Folder name, count of images in "fnd" local folder)
        for filename in os.listdir(fnd_root):

            local_fnd_path = os.path.join(fnd_root, filename)
            remote_img_path = remote_dir_path + "/" + filename
            ### sftp put command will OVERWRITE if file already exists. Hence no check before put to make this Idempotent  ####
            try:
                sftp.put(local_fnd_path, remote_img_path)
                print(f" {local_fnd_path}  copied to {remote_img_path}")
            except Exception as e:
                print(f"Error copy file {remote_img_path} remote server")
            # print(f"Processing: {file_path}")

    return (filtered_image_count)


def update_ctrl_data(folders_to_process, json_ctrl_data, remote_dir_root, override_run, sftp):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => update_ctrl_data\n')
    ## update JSON Control File ##
    if override_run:
        # the override key is formed by concatenating the root(CAM1/2) with "-OVR"(SSAK-245923-BAAEC-CAM1-OVR).
        # Value is reset to EMPTY so next scheduled run does not pick up any override values

        json_ctrl_data[remote_dir_root + "-OVR"] = []
        print(f'\nIn override ctrl section {json_ctrl_data}')
    else:
        process_run_date_time = datetime.datetime.now(ZoneInfo("Europe/London"))
        json_ctrl_data["last_run_date"] = process_run_date_time.strftime("%m/%d/%Y")
        json_ctrl_data["last_run_time"] = process_run_date_time.strftime("%H:%M:%S")
        last_proc_folder = max(folders_to_process)
        json_ctrl_data[remote_dir_root] = last_proc_folder
        print(f'\nIn normal ctrl section {json_ctrl_data}\n')

    with open("ctrl_data3.json", 'w') as file:
        json.dump(json_ctrl_data, file, indent=4)

    remote_ctrl_path = 'Logs/ctrl_data3.json'
    local_ctrl_path = 'ctrl_data3.json'
    try:
        sftp.put(local_ctrl_path, remote_ctrl_path)
        print("CTRL DATA UPDATE SUCCESS")
    except Exception as e:
        print(f"ERROR UPDATING CTRL DATA")


def generate_report(start_time, raw_image_count, filtered_image_count, gemini_error_count, override_run):
    print(f'\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => generate_report\n')
    end_time = datetime.datetime.now(ZoneInfo("Europe/London"))
    run_time = end_time - start_time
    # Convert run time to string and then split at first "." and then take the first part i.e. [0] occurence.
    # This is done to strip the milliseconds at the end .
    run_time_str = str(run_time).split('.')[0]

    # report header  is based on override run to differentiate from normal run
    if override_run:

        report = (f"OVERRIDE SPECIAL RUN - SHELDUCK IMAGE FILTER REPORT FOR {start_time.strftime('%d/%m/%Y')}\n\n")
    else:
        report = (f"SHELDUCK IMAGE FILTER REPORT FOR {start_time.strftime('%d/%m/%Y')}\n\n")

    report += f"Process Run Date    : {start_time.strftime('%d/%m/%Y')}\n"
    report += f"Process Start Time  : {start_time.strftime('%H:%M:%S')}\n"
    report += f"Process End Time    : {end_time.strftime('%H:%M:%S')}\n"
    report += f"Process Run Time    : {run_time_str}\n"
    raw_image_count_sorted = sorted(raw_image_count, key=lambda x: (x[0], x[
        1]))  # sorts each tuple (CAM , FOLDER, COUNT) in list by CAM and FOLDER Ex. [('SSAM-452149-BFDCD-CAM2', '20250125', 75)]
    image_count_sorted = sorted(filtered_image_count, key=lambda x: (
    x[0], x[1]))  # sorts each tuple (CAM , FOLDER, COUNT) in list by CAM and FOLDER
    gemini_error_sorted = sorted(gemini_error_count, key=lambda x: (x[0], x[1]))
    print(f"sorted raw  {raw_image_count_sorted}")
    print(f"sorted filt {image_count_sorted}\n")

    # Loop through both lists in parallel using zip
    prev_cam = ''

    if (raw_image_count_sorted) and (
    image_count_sorted):  # ZIP function combines 2 lists so we can iterate thru them in parallel
        for raw, filtered, gemerror in zip(raw_image_count_sorted, image_count_sorted,
                                           gemini_error_sorted):  # Ex.  sorted raw [('SSAK-245923-BAAEC-CAM1', '20250125', 2), ('SSAM-452149-BFDCD-CAM2', '20250126', 52)]
            # Ex.  sorted fil [('SSAK-245923-BAAEC-CAM1', '20250125', 0), ('SSAM-452149-BFDCD-CAM2', '20250126', 22)]
            cam, folder, raw_count = raw  # Unpack the 3 elements of each tuple in the list raw list
            _, _, filtered_count = filtered
            _, _, gem_error = gemerror  # Unpack the 3 elements of each tuple in the list filtered list. Ignore first 2 elements
            if prev_cam != cam:  # since both lists are sorted by CAM, the CAM header prints when change in CAM(CAM1 vs CAM2)
                report += f"\n FOLDERS UNDER {cam}\n"
                report += f" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
                prev_cam = cam
            report += f"    Folder                     : {folder}\n"
            report += f"        Total Raw Images       : {str(raw_count).zfill(4)}\n"
            report += f"        Total Filtered Images  : {str(filtered_count).zfill(4)}\n"
            report += f"        Total Gemini Errors    : {str(gem_error).zfill(4)}\n"

    else:
        report += f"!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!\n"
        report += f"!NO FOLDERS WERE ELIGIBLE TO BE PROCESSED FOR TODAY!\n"
        report += f"!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!\n"
    return report


def send_email(report, override_run):
    print(f'[{datetime.datetime.now().strftime('%H:%M:%S')}]  : Executing Function => send_email\n')

    report_date = datetime.datetime.now(ZoneInfo("Europe/London"))
    report_date_fmt = report_date.strftime('%d/%m/%Y')
    # Sender and receiver email addresses
    sender_email = "mankoni@gmail.com"
    password = os.getenv(
        'GMAIL_APP_PASSWORD')  # we pass the KEY of the Windows Environment variable that has the password value
    receiver_email = ["m.w.steele.1972@gmail.com", "shanki@yahoo.com", "HughBarn@Hotmail.com"]  # List of emails

    # Create the email message
    if override_run:
        subject = "ShelDuck OVERRIDE Report - " + report_date_fmt
    else:
        subject = "ShelDuck Daily Report - " + report_date_fmt
    body = report

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_email)  # Join the list into a single string for the 'To' field
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Gmail SMTP server details
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    try:
        # Set up the server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Secure the connection

        # Log in to the Gmail account

        server.login(sender_email, password)

        server.sendmail(sender_email, receiver_email, msg.as_string())  # List of emails for recipients
        print("Email sent successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the server connection
        server.quit()


def main():
    ###          GLOBAL VARIABLES           ###
    remote_ctrl_path = 'Logs/ctrl_data3.json'
    remote_dir_roots = ['SSAK-245923-BAAEC-CAM1', 'SSAM-452149-BFDCD-CAM2']  # The directory on the SFTP server
    local_dir_root = "C:/Users/shanki/Downloads/DuckDataset"  # Set it what ever your local dir
    filtered_image_count = []  # used to store number of filtered (match) in each camera by folder. ONLY for reporting use
    raw_image_count = []  # used to store number of images downloaded in each camera by folder. ONLY for reporting use
    gemini_error_count = []  # used to store error count in gemini API calls. ONLY for reporting use

    sftp = create_sftp_connection()  # Function call returns SFTP client needed to get and put files to remote server
    json_ctrl_data = get_ctrl_json(sftp, remote_ctrl_path)
    override_run = False
    start_time = datetime.datetime.now(ZoneInfo("Europe/London"))
    ###         END GLOBAL VARIABLES         ###
    override_run = check_override_folders(json_ctrl_data, remote_dir_roots)


    for remote_dir_root in remote_dir_roots:
        ### OVERRIDE FILTER LOGIC ###
        if override_run:
            override_key = remote_dir_root + "-OVR"
            folders_to_process = json_ctrl_data[override_key]
            print(f'OVERRIDE FOLDERS TO PROCESS  for CAMERA {remote_dir_root} FOLDER LIST {folders_to_process}')
        ### END OVERRIDE FILTER LOGIC ###
        else:

            remote_folders_list = get_remote_folders_list(sftp, remote_dir_root)
            print(f"list of remote folders for CAM {remote_dir_root}  ==>  {remote_folders_list}")
            last_processed_folder = json_ctrl_data[remote_dir_root]
            print(f'Last proc folder for Camera {remote_dir_root} is ==> {last_processed_folder}\n')
            folders_to_process = derive_remote_folders_to_process(last_processed_folder, remote_folders_list)
            print(f'Folders TO BE processed for Camera {remote_dir_root} is ==> {folders_to_process}\n\n')
        if folders_to_process:  # check if the folder list is not empty only then proceed with rest of func calls

            rename_remote_folders(folders_to_process, remote_dir_root, sftp)
            raw_image_count = download_images_to_local(folders_to_process, local_dir_root, remote_dir_root,
                                                       raw_image_count, sftp)
            print(f"Raw Image  Counts {raw_image_count}")
            gemini_error_count = asyncio.run(
                process_images(folders_to_process, local_dir_root, remote_dir_root, gemini_error_count))
            sftp = create_sftp_connection()  # Reconnect with remote as it was closed after remote copy to local to avoid timing out during Gemini process
            filtered_image_count = copy_filtered_images_local_to_remote(folders_to_process, local_dir_root,
                                                                        remote_dir_root, sftp, filtered_image_count)
            print(f"\nFiltered Image Counts {filtered_image_count}")
            update_ctrl_data(folders_to_process, json_ctrl_data, remote_dir_root, override_run, sftp)
    report = generate_report(start_time, raw_image_count, filtered_image_count, gemini_error_count, override_run)
    print(report)
    print(f'Gemini errors {gemini_error_count}')
    # send_email(report, override_run)
    sftp.close()
    print("sftp closed")


if __name__ == "__main__":
    main()


