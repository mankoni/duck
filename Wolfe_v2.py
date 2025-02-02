
import paramiko
import google.generativeai as genai
import asyncio
from PIL import Image
import os
import time
import pytz
import datetime
import json



# SFTP credentials and server details
hostname = 'sftp.adrive.com'  # e.g., 'sftp.example.com'
port = 22  # Default SFTP port
username = 'HughBarn@Hotmail.com'  # Your SFTP username
password = 'n0Password'  # Your SFTP password


uk_timezone = pytz.timezone('Europe/London')
# Current time in UK timezone

# Format the time for the report
process_run_date = datetime.datetime.now(uk_timezone)
start_time       = datetime.datetime.now(uk_timezone)


# Function to download the list of folders to be processed to local drive
# list of folders determined  by comparing the last downloaded folder to the
# complete list of remote folders
# NOTE: The term folder refers the directory that contains the image files
#       In case of remote server we call "20241201" as a folder whose path is
#       "SSAK-245923-BAAEC-CAM1/20241201"
#       The folder name is assumed to be always in YYYYMMDD format
############################################################################


def download_remote_folders_to_process():
    remote_dir_root = 'SSAK-245923-BAAEC-CAM1'  # The directory on the SFTP server
    remote_ctrl_path = 'Logs/ctrl_data.json'
    local_dir_root = "C:/Users/shanki/Downloads/DuckDataset"  # Set it what ever your local dir
    folders_to_be_processed = []
    raw_image_count =[]
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
        print(f"Connected to {hostname}. Listing folders in {remote_dir_root}...")

        # List files in the remote directory
        # print(f"Changed to directory: {remote_dir_root}")
        remote_folders = sftp.listdir(remote_dir_root)
        remote_folders.sort()
        # print("remote folders ", remote_folders)
        sftp.get(remote_ctrl_path,'ctrl_data.json')
        with open('ctrl_data.json', 'r') as f:
            data = json.load(f)
            last_folder = (data['last_proc_folder'])
            print("last folder from ctrl json " , last_folder)


        # We obtain a list of folders from the remote folder GREATER than the last processed folder
        # obtained from the ctrl_data.JSON using list comprehension loop below

        difference = [fold for fold in remote_folders if fold > last_folder]
        print("missing local folders ", len(difference))
        difference.sort()
        difference = difference[:1]
        print(difference)

        # Download the files from unprocessed remote folders while simultaneously creating the local folders
        #
        # 1) Iterate thru each unprocessed folder  (LOOP #1 only if folder count  > 0)
        #  1.1)  construct new path by concatenating root + {folder} + "mages"
        #   Eg: of remote_dir path: SSAK-245923-BAAEC-CAM1/20241122/images
        #  1.2)  Since the folder does NOT exist locally, create the local folder
        #  1.3)  Get list of files in the remote folder
        #  2)     Iterate thru each file which is now the actual image file (LOOP # 2 only if file count > 0)
        #    2.1)      Store the LOCAL image folder path to folders_to_be_processed array. This array will
        #              serve( as input to the next phase for Image identification)
        #    2.1)      Create the path for remote and local by concatenating remote_dir and local_dir with
        #              the image file name
        #    2.2)      CALL sftp.get to copy file from remote to local

        if difference:
            for folder in difference:
                remote_dir = remote_dir_root + "/" + folder + "/images"
                local_dir = local_dir_root + "/" + folder
                os.mkdir(local_dir)
                print("local folder create ", local_dir)

                remote_files = sftp.listdir(remote_dir)
                print(f"found {len(remote_files)}  in remote dir {remote_dir}")
                raw_image_count.append(len(remote_files))

                if remote_files:
                    # print("Processing files in remote Dir --> ", remote_dir)
                    folders_to_be_processed.append(folder)
                    for imagefile in remote_files:
                        print(imagefile)
                        remotefile = remote_dir + "/" + imagefile
                        localfile = local_dir + "/" + imagefile
                        sftp.get(remotefile, localfile)

                else:
                    print("No files found in the remote directory.", remote_dir)

            # Close the SFTP session and SSH connection (after listing new dir files)
        sftp.close()
        ssh.close()

    except paramiko.AuthenticationException as auth_error:
        print(f"Authentication failed: {auth_error}")
    except paramiko.SSHException as ssh_error:
        print(f"SSH connection error: {ssh_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()  # Print the detailed traceback of the error
    return folders_to_be_processed , raw_image_count




###############   START OF STEP2 CALLING GEMINI FLASK 1.5  #################
## This async function takes one LOCAL folder created in the prev func one at a time and loops thru each image in that folder
## Calls model for each image. If match not found delete the image from folder. Thus at the end of the process
## only matched images will remain in the folder. This will be then copied to the remote server in the next function
#############################################################################################################################
async def process_images(process_folder):
    genai.configure(api_key="AIzaSyCkMVDojZ82ciCBo2VqSlBFe2ebU-74KdM")
    model = genai.GenerativeModel("gemini-1.5-flash")


    folder_path = "C:/Users/shanki/Downloads/DuckDataset/" + process_folder
    ntf_root = "C:/Users/shanki/Downloads/DuckDataset/duck_ntf"
    und_root = "C:/Users/shanki/Downloads/DuckDataset/duck_und"
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        ntf_path  = os.path.join(ntf_root,filename)
        und_path = os.path.join(und_root, filename)
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

            print(f"Response resolved: {response.text}")
            # if response.text == "NO":
            # if "No" in response.text:
            #     print(F'Duck not found | {file_path} | No | {response.text}')
            #     img.close()
            #
            #     try:
            #         os.rename(file_path, ntf_path)
            #         # print(f"{file_path} has been deleted.")
            #     except FileNotFoundError:
            #         print(f"The file {file_path} was not found.")
            # elif "Yes" in response.text:
            #     print(F'Duck  found | {file_path} | Yes | {response.text}')
            # else:
            #     print(F'Cannot Determine | {file_path} | N/A | {response.text}')
            #     img.close()
            #     try:
            #         os.rename(file_path, und_path)
            #         # print(f"{file_path} has been deleted.")
            #     except FileNotFoundError:
            #         print(f"The file {file_path} was not found.")




            # Output the result in markdown
            # to_markdown(response.text)


            # sleep for 3 sec since Google API has a limit of 15 req/min
            time.sleep(3)
        except Exception as e:
            # Catch any errors during the response generation and processing
            print(f"Error processing {filename}: {e}")

###############   START OF STEP3 Copy filtered image files from local to remote  #################
### This func takes in a arg of list of processed local folders from the prev func
### The local folders contain ONLY matched images. These images are then copied to
### remote server under a new sub dir called "FILTERED IMAGES"
### This func also updates the ctrl_data.json file on the remote server with the current run info
#################################################################################################

def copy_filtered_images_local_to_remote(pf):
    remote_dir_root = 'SSAK-245923-BAAEC-CAM1'
    filtered_image_count = []

    try:
        # Set up the SSH client
        ssh = paramiko.SSHClient()

        # Automatically add the SFTP server's host key (optional for security)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the server
        print(f"Connecting to {hostname}...")
        ssh.connect(hostname, port=port, username=username, password=password)

        # Start the SFTP session
        sftp = ssh.open_sftp()

        for folder in pf:


            remote_dir_path = remote_dir_root + "/" + folder + "/" + "FILTERED IMAGES"
            sftp.mkdir(remote_dir_path)

            local_dir_path = "C:/Users/shanki/Downloads/DuckDataset/" + folder
            local_files = os.listdir(local_dir_path)
            filtered_image_count.append(len(local_files))
            for image in os.listdir(local_dir_path):
                local_img_path  = local_dir_path  + "/" +image
                remote_img_path = remote_dir_path + "/" +image

                # print("local image to be copied " , local_img_path)
                # print("remote image copied to  "  , remote_img_path)

                sftp.put( local_img_path,remote_img_path)

            ######    write Control JSON file   #######

            now = datetime.datetime.now(uk_timezone)
            last_run_date = now.strftime('%d/%m/%Y')
            last_run_time = now.strftime('%H:%M:%S')
            ctrl_data = {
                "last_run_date": last_run_date,
                "last_run_time": last_run_time,
                "last_proc_folder": max(pf)
            }

            remote_ctrl_dir = 'Logs'
            file_path = "ctrl_data.json"
            with open(file_path, "w") as f:
                json.dump(ctrl_data, f)
            sftp.put(file_path, remote_ctrl_dir + "/" + file_path, confirm=True)

        sftp.close()
        ssh.close()

    except paramiko.AuthenticationException as auth_error:
        print(f"Authentication failed: {auth_error}")
    except paramiko.SSHException as ssh_error:
        print(f"SSH connection error: {ssh_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()  # Print the detailed traceback of the error
    return filtered_image_count

def main():
    folder_list , raw_image_count = download_remote_folders_to_process()
    print("Local Dir to be processed:--> ", folder_list)
    print("Raw Image count --> " , raw_image_count)
    if folder_list:

        for folder in folder_list:
            asyncio.run(process_images("folder"))

    filtered_image_count = copy_filtered_images_local_to_remote(folder_list)
    print("Filtered Image count --> ", filtered_image_count)

    #####    REPORT CREATION STARTS HERE    ###########
    now = datetime.datetime.now(uk_timezone)
    # Format the time for the report
    end_time = datetime.datetime.now(uk_timezone)
    run_time = end_time - start_time
    # Convert run time to string and then split at first "." and then take the first part i.e. [0] occurence.
    # This is done to strip the milliseconds at the end .
    run_time_str = str(run_time).split('.')[0]


    # Generate the report using f-string with explicit newlines
    report = (f"SHELDUCK IMAGE FILTER REPORT FOR {process_run_date.strftime('%d/%m/%Y')}\n" 
              f"Process Run Date    : {process_run_date.strftime('%d/%m/%Y')}\n"
              f"Process Start Time  : {start_time.strftime('%H:%M:%S')}\n"
              f"Process End Time    : {end_time.strftime('%H:%M:%S')}\n"
              f"Process Run Time    : {run_time_str}\n"
              f"\nFolders Processed   : ")

    # Iterate through the folder data
    if folder_list:
        report += f"\n"
        for i in range(len(folder_list)):
            folder = folder_list[i]
            raw_count = raw_image_count[i]
            filtered_count = filtered_image_count[i]

            # Append folder data to the report with explicit newlines
            report += f"\n{folder}\n"
            report += f"    Total Raw Images       : {raw_count}\n"
            report += f"    Total Filtered Images  : {filtered_count}\n"
    else:
        report += "NONE"


    # Print the final report
    print(report)
    report_file_name = "report_"+ process_run_date.strftime('%Y%m%d')+".txt"
    with open(report_file_name, "w") as file:
        file.write(report)

    try:
        # Set up the SSH client
        ssh = paramiko.SSHClient()

        # Automatically add the SFTP server's host key (optional for security)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the server
        print(f"Connecting to {hostname}...")
        ssh.connect(hostname, port=port, username=username, password=password)

        # Start the SFTP session
        sftp = ssh.open_sftp()
        remote_ctrl_dir = 'Logs'
        file_path = report_file_name

        sftp.put(file_path, remote_ctrl_dir + "/" + file_path, confirm=True)
        sftp.close()
        ssh.close()
    except Exception as e:
     print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()