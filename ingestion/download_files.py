"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/03/2020
"""

import os
import time
import requests
import lxml.html as lh
from zipfile import ZipFile
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
        Generate a list of name of the GDELT data files stored on the web site.
        There are version 1.0 & version 2.* GDELT data files, each version
        requires different approach for extracting file links & names.
        """
        file_list = []

        if data_version == "v2":
            # master text file already lists all GDELT v2 file links,
            # so no need to crawl GDELT web site for that.
            master_file_name = "masterfilelist.txt"

            # events_list = []
            # mentions_list = []
            # gkg_list = []

            # create a GET response object
            r_obj = requests.get(url + master_file_name)

            # save the GDELT v2 file list locally
            with open("./data/" + master_file_name, "wb") as f_obj:
                f_obj.write(r_obj.content)
            
            with open("./data/" + master_file_name, "r") as file_content:
                k = 0
                for file_line in iter(file_content.readline, ""):
                    if k >= 9: break
                    k += 1
                    # extract GDELT file url only
                    file_name = file_line.split()[-1]
                    # extract file name from its url
                    file_name = file_name.split("/")[-1]

                    file_list.append(file_name)
                    # if file_name.endswith(".export.CSV.zip"):
                    #     events_list.append(file_name)
                    # elif file_name.endswith(".mentions.CSV.zip"):
                    #     mentions_list.append(file_name)
                    # elif file_name.endswith(".gkg.csv.zip"):
                    #     gkg_list.append(file_name)
        else:
            # get the list of all the links on the GDELTv1 web page
            page = requests.get(url + 'index.html')
            doc = lh.fromstring(page.content)
            link_list = doc.xpath("//*/ul/li/a/@href")

            # Right now getting only files create after 2013-04-01,
            # because other ones have a different naming convention
            file_list = [x for x in link_list if x.endswith(".export.CSV.zip")]
           
        return file_list


    def download_data(self, base_url, bucket_name, file_list,):
        """
        Download data files from GDELT web site,
        save them to an EC2 instance & unzip, then
        save unzipped files to the AWS S3 storage
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


    def run(self):
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