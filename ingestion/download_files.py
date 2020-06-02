"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   04/03/2020
"""

from zipfile import ZipFile, BadZipFile
import os
import logging
import time
import requests
import lxml.html as lh
import boto3


class DownloadFiles():

    def __init__(self):
        logging.basicConfig(filename='../logs/download_files.log', filemode='w', level=logging.INFO)
        # GDELT v2 config
        self.gdelt_base_url = 'http://data.gdeltproject.org/gdeltv2/'
        self.bucket_name = 'gdelt-v2'
        self.data_folder = "../data/"
        self.master_file_name = "masterfilelist.txt"
        self.prev_master_name = "PREVIOUS-" + self.master_file_name
        self.delta_master_name = "DELTA-" + self.master_file_name
        # GDELT v1 config
        # self.gdelt_base_url = 'http://data.gdeltproject.org/events/'
        # self.bucket_name = 'gdelt-v1'


    def generate_file_list(self, url, data_version):
        """
        Generate the GDELT data files url list stored on the GDELT web site.
        Master text file already lists all GDELT v2 file links, so no need to
        crawl GDELT web site for that.
        Also, there is GDELT v1.0 & v2.0 data files, each version  requires a
        different approach for extracting file links & names.
        If the ingestion process run for the first time, we simply download the
        masterfile list, otherwise generate a differences file and only load the
        new GDELT files.

        Args:
            url (string) - path to master file list on the GDELT website
            data_version (string) - specifies the GDELT dataset version to download
        Returns:
            file_list (list) - list of GDELT data file names
        """
        file_list = []
        local_master_path = self.data_folder + self.master_file_name
        local_prev_master_path = local_master_path

        if data_version == "v2":
            # check if the master file already exists then generate the difference file,
            # otherwise use the downloaded master data file as is
            if os.path.exists(local_master_path):
                local_prev_master_path = self.data_folder + self.prev_master_name
                os.rename(local_master_path, local_prev_master_path)            
            
            # create a GET response object
            r_obj = requests.get(url + self.master_file_name)
                
            # save the newest master file locally
            with open(local_master_path, "wb") as f_obj:
                f_obj.write(r_obj.content)

            # generate the DELTA master file list, if previous master file exists
            if local_prev_master_path != local_master_path:
                cmd_to_execute = 'diff --changed-group-format="%> " \
                                       --unchanged-group-format="" {0} {1} > {2}' \
                                 .format(local_prev_master_path, \
                                         local_master_path, \
                                         self.data_folder + self.delta_master_name)
                os.system(cmd_to_execute)
                local_master_path = self.data_folder + self.delta_master_name
            
            with open(local_master_path, "r") as file_content:
                for file_line in iter(file_content.readline, ''):
                    if len(file_line.strip()) > 0:
                        # extract GDELT file url only
                        file_name = file_line.split()[-1]
                        # extract file name from its url
                        file_name = file_name.split("/")[-1]
                        file_list.append(file_name)

            if os.path.exists(local_prev_master_path) and \
               os.path.exists(self.data_folder + self.delta_master_name):
                os.remove(local_prev_master_path)
                os.remove(self.data_folder + self.delta_master_name)
 
        elif data_version == "v1":
            # get the list of all the links on the GDELTv1 web page
            page = requests.get(url + 'index.html')
            doc = lh.fromstring(page.content)
            link_list = doc.xpath("//*/ul/li/a/@href")
            # Right now get only the files created after 2013-04-01,
            # Earlier years have a different naming convention.
            file_list = [x for x in link_list if x.endswith(".export.CSV.zip")]

        return file_list


    def download_data(self, base_url, bucket_name, file_list):
        """
        Downloads data files from GDELT web site, saves it to an EC2 instance
        then unzips the files and saves to the AWS S3 storage

        Args:
            base_url (string) -  path to the GDELT dataset on the website
            bucket_name (string)  - name of the AWS S3 bucket for storing the data files
            file_list (string) - list of GDELT data file names
        Returns:
            None.
        """
        for file in file_list:
            # download files from the GDELT website
            r = requests.get(base_url + file) # create HTTP response object 
            
            # send a HTTP request to the EC2 server and save
            # the HTTP response in a response object
            with open(self.data_folder + file, 'wb') as f:  
                f.write(r.content)

            # unzip the downloaded file to the same folder
            try:
                with ZipFile(self.data_folder + file, 'r') as zip_obj:
                    zip_obj.extractall(self.data_folder)

                key = file[:-4]    #file name w/o ".zip"
                file_path = self.data_folder + key 
                
                # connect to S3 storage and move the unzipped data file there
                s3 = boto3.resource(service_name = 's3')
                s3.create_bucket(Bucket=bucket_name)
                s3.meta.client.upload_file(file_path, bucket_name, key)

                print("'" + key + "'" + " added to S3.")
                logging.info("'" + key + "'" + " added to S3.")

                # delete both ziped/unzipped data files from the EC2 server
                if os.path.exists(self.data_folder + key):
                    os.remove(self.data_folder + file)
                    os.remove(self.data_folder + key)
                else:
                    print("The file doesn't exist!")
                    logging.warning("The file doesn't exist!")
            except BadZipFile:
                print("\nCorrupt '" + self.data_folder + file + "' archive! Skipped.\n")
                logging.warning("\nCorrupt '" + self.data_folder + file + "' archive! Skipped.\n")


    def run(self):
        """
        Method that outlines and executes the workflow of this class

        Args:
            None.
        Returns:
            None.
        """
        file_list = []

        print('\n========== GDELT data transfer started! ==========\n')
        logging.info('\n========== GDELT data transfer started! ==========\n')

        start_time = time.time()
        file_list = self.generate_file_list(self.gdelt_base_url, "v2")

        print("Master file lines read: " + str(len(file_list)) )
        logging.info("Master file lines read: " + str(len(file_list)) )

        self.download_data(self.gdelt_base_url, self.bucket_name, file_list)
        end_time = time.time()

        print("----------------------------------")
        logging.info("----------------------------------")
        print("Execution time: %s seconds" % round(end_time - start_time, 2))
        logging.info("Execution time: %s seconds" % round(end_time - start_time, 2))
        print('\n========== GDELT data transfer completed! ==========\n')
        logging.info('\n========== GDELT data transfer completed! ==========\n')

###################### End of class DownloadFiles ########################
#######################################################################################

def main():
    process = DownloadFiles()
    process.run()
    

if __name__ == '__main__':
    main()