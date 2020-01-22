"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   10/28/2019
"""

import os
import time
import requests
import lxml.html as lh
from zipfile import ZipFile
import boto3



def generate_file_list(url):
    """
    Generate a list of name of the GDELT data files stored on the web site
    """
    # get the list of all the links on the gdelt web page
    page = requests.get(url + 'index.html')
    doc = lh.fromstring(page.content)
    link_list = doc.xpath("//*/ul/li/a/@href")

    # Right now getting only the files newer than 2013-04-01,
    # because other ones have a different naming convention
    file_list = [x for x in link_list if x.endswith(".export.CSV.zip")]

    return file_list


def download_data(base_url, file_list, bucket):
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
            #print("File " + file + " downloaded!")

        # unzip the downloaded file to the same folder
        with ZipFile(in_folder + file, 'r') as zipObj:
            zipObj.extractall(in_folder)
            #print("File " + file + " extracted!")
        

        # connect to S3 storage and move the unzipped data file there
        s3 = boto3.resource(service_name = 's3')
        file_path = in_folder + file[:-4] 
        key = file[:-4]
        s3.meta.client.upload_file(file_path, bucket, key)
        print("'" + file[:-4] + "'" + " uploaded to S3.")

        # delete both ziped/unzipped data files from the EC2 server
        if os.path.exists(in_folder + file[:-4]):
            os.remove(in_folder + file)
            os.remove(in_folder + file[:-4])
            #print("File(s) " + file[:-4] + ".* removed!\n")
            #print("---------------------------------\n")
        else:
            print("The file doesn't exist!")


if __name__ == '__main__':
    gdelt_base_url = 'http://data.gdeltproject.org/events/'
    bucket = 'gdelt-1'

    print('\n========== GDELT data transfer started! ==========\n')

    start_time = time.time()
    file_list = generate_file_list(gdelt_base_url)
    download_data(gdelt_base_url, file_list, bucket)
    end_time = time.time()

    print("----------------------------------")
    print("Execution time: %s seconds\n" % round(end_time - start_time, 2))

    print('\n========== GDELT data transfer ended! ==========\n')