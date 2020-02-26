"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/03/2020
"""

import os
import time
import requests
import lxml.html as lh
from zipfile import ZipFile, BadZipFile
import boto3

class DownloadFiles():
    def __init__(self):
        # GDELT v1 config
        # self.gdelt_base_url = 'http://data.gdeltproject.org/events/'
        # self.bucket_name = 'gdelt-v1'

        # GDELT v2 config
        self.gdelt_base_url = 'http://data.gdeltproject.org/gdeltv2/'
        self.bucket_name = 'gdelt-v2'


    def generate_file_list(self, url, data_version):
        """
        Generate the GDELT data files url list stored on the GDELT web site.
        There is GDELT v1.0 & v2.0 data files, each version  requires
        a different approach for extracting file links & names.

        Args:
            url (string) - path to master file list on the GDELT website
            data_version (string) - specifies the GDELT dataset version to download
        Returns:
            file_list (list) - list of GDELT data file names
        """
        file_list = []

        if data_version == "v2":
            # master text file already lists all GDELT v2 file links,
            # so no need to crawl GDELT web site for that.
            master_file_name = "masterfilelist.txt"

            # create a GET response object
            r_obj = requests.get(url + master_file_name)

            # save the GDELT v2 file list locally
            with open("./data/" + master_file_name, "wb") as f_obj:
                f_obj.write(r_obj.content)
            
            with open("./data/" + master_file_name, "r") as file_content:
                k = 0
                for file_line in iter(file_content.readline, ""):
                    # if k >= 9: break
                    k += 1
                    # extract GDELT file url only
                    file_name = file_line.split()[-1]
                    # extract file name from its url
                    file_name = file_name.split("/")[-1]

                    file_list.append(file_name)
        else:
            # get the list of all the links on the GDELTv1 web page
            page = requests.get(url + 'index.html')
            doc = lh.fromstring(page.content)
            link_list = doc.xpath("//*/ul/li/a/@href")

            # Right now get only the files created after 2013-04-01,
            # Earlier versions have a different naming convention.
            file_list = [x for x in link_list if x.endswith(".export.CSV.zip")]
           
        return file_list


    def download_data(self, base_url, bucket_name, file_list,):
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
        in_folder = "./data/"

        for file in file_list:
            # download files from the GDELT website
            r = requests.get(base_url + file) # create HTTP response object 
            
            # send a HTTP request to the EC2 server and save
            # the HTTP response in a response object
            with open(in_folder + file, 'wb') as f:  
                f.write(r.content)

            # unzip the downloaded file to the same folder
            try:
                with ZipFile(in_folder + file, 'r') as zip_obj:
                    zip_obj.extractall(in_folder)

                file_path = in_folder + file[:-4] 
                key = file[:-4]    #file name w/o ".zip"

                # connect to S3 storage and move the unzipped data file there
                s3 = boto3.resource(service_name = 's3')
                s3.create_bucket(Bucket=bucket_name)
                s3.meta.client.upload_file(file_path, bucket_name, key)

                print("'" + file[:-4] + "'" + " added to S3.")

                # delete both ziped/unzipped data files from the EC2 server
                if os.path.exists(in_folder + file[:-4]):
                    os.remove(in_folder + file)
                    os.remove(in_folder + file[:-4])
                else:
                    print("The file doesn't exist!")

            except BadZipFile:
                print("\nCoorupt '" + in_folder + file + "' archive! Skipped.\n")


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

        start_time = time.time()
        file_list = self.generate_file_list(self.gdelt_base_url, "v2")
        self.download_data(self.gdelt_base_url, self.bucket_name, file_list)
        end_time = time.time()

        print("----------------------------------")
        print("Execution time: %s seconds" % round(end_time - start_time, 2))
        print('\n========== GDELT data transfer completed! ==========\n')

###################### End of class DownloadFiles ########################
#######################################################################################

def main():
    process = DownloadFiles()
    process.run()
    

if __name__ == '__main__':
    main()