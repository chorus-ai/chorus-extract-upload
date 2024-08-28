import os
from datetime import datetime
import random
import shutil
import numpy as np
import glob

def generateFirstFiles():
    # need: folder for test data
    rootfolder = "TestData"
    if not os.path.exists(rootfolder):
        os.mkdir(rootfolder)

    # need: folder for initial push
    firstupload_string = "SiteFolder_FirstSnapshot"

    initpushdir = os.path.join(rootfolder,firstupload_string)
    if not os.path.exists(initpushdir):
        os.mkdir(initpushdir)

    # need: person_id folders in initial push
    # seed so that everything happens the same
    random.seed(0)
    numfolders = 10
    personids = [random.randint(1000000,2000000) for i in range(numfolders)]
    print(personids)

    numfiles = 10

    unix0_2023 = 1672549200
    unix0_2024 = 1704085200

    # waveform directory
    for modality in ["Waveforms", "Images"]:

        for id in personids:
            subdir = str(random.randint(10000,20000)) if modality == "Waveforms" else os.path.join(str(random.randint(10000,20000)),str(random.randint(10000,20000)))
            personwfdir = os.path.join(initpushdir, str(id), modality, subdir)
            os.makedirs(personwfdir, exist_ok=True)
                
            # need: files corresponding to a pt

            intervalpts = [random.randint(unix0_2023,unix0_2024) for i in range(numfiles*2)]
            intervalpts.sort()
            print(intervalpts)
            
            for filenum in range(numfiles):
                interval_st = intervalpts[2*(filenum)]
                interval_end = intervalpts[2*(filenum)+1]
                interval_dur = interval_end - interval_st
                datestr = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                fileSizeInBytes = 1024
                # outputfilename = str(id) +  "_" + str(random.randint(10000,20000)) + "_"
                outputfilename = str(id) +  "_" + datestr + "_" + ((str(interval_dur) + ".dat") if modality == "Waveforms" else ("CT_01_" + str(filenum) + ".dcm"))
                outputfilepath = os.path.join(personwfdir, outputfilename)
                with open(outputfilepath, 'wb') as fout:
                    fout.write(os.urandom(fileSizeInBytes)) 

    # OMOP directory
    personwfdir = os.path.join(initpushdir, "OMOP")
    os.makedirs(personwfdir, exist_ok=True)
    
    for filenum in range(14):
        fileSizeInBytes = 1024
        outputfilename = str(filenum) + ".csv"
        outputfilepath = os.path.join(personwfdir, outputfilename)
        with open(outputfilepath, 'wb') as fout:
            fout.write(os.urandom(fileSizeInBytes)) 
            
            
def generateUpdate():
    # now do the same, but  make a new push that selectively keeps some files 
    random.seed(0)

    rootfolder = "TestData"
    if not os.path.exists(rootfolder):
        os.mkdir(rootfolder)

    # need: folder for initial push
    firstupload_string = "SiteFolder_FirstSnapshot"

    initpushdir = os.path.join(rootfolder,firstupload_string)

    fileSizeInBytes = 1024

    secondupload_string = "SiteFolder_SecondSnapshot"

    secpushdir = os.path.join(rootfolder,secondupload_string)
    if not os.path.exists(secpushdir):
        os.mkdir(secpushdir)

    unix0_2023 = 1672549200
    unix0_2024 = 1704085200

    personids = [int(i) for i in os.listdir(initpushdir) if i != "OMOP"]
    
    numfiles = 10
    
    # waveforms
    for modality in ["Waveforms", "Images"]:
        for id in personids:
            personwfdir_1 = os.path.join(initpushdir, str(id), modality)
            
            personwfdir_2 = os.path.join(secpushdir, str(id), modality)
            os.makedirs(personwfdir_2, exist_ok=True)
            
            # existing files
            files = glob.glob(os.path.join(personwfdir_1, "**", "*"), recursive=True)

            # need: files corresponding to a pt
            for origpath in files:
                if os.path.isdir(origpath):
                    continue
                
                # do nothing
                # delete and add a new file
                # modify contents
                # symlink
                # add new directory
                option = random.randint(0,3)
                
                curfile = os.path.relpath(origpath, personwfdir_1)
                dest_path = os.path.join(personwfdir_2, curfile)
                dest_dir = os.path.dirname(dest_path)
                os.makedirs(dest_dir, exist_ok=True)
                
                # copy as is
                if option == 0:
                    shutil.copy(origpath, dest_path)
                    
                # delete this file and add a file in same directory
                elif option == 1:
                    interval_st = random.randint(unix0_2023,unix0_2024)
                    interval_end = random.randint(unix0_2023,unix0_2024)
                    if(interval_st > interval_end):
                        temp = interval_st
                        interval_st = interval_end
                        interval_end = temp
                    interval_dur = interval_end - interval_st
                    datestr = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                    newpath = str(id) +  "_" + datestr + "_" + ((str(interval_dur) + ".dat") if modality == "Waveforms" else ("CT_01_" + str(filenum) + ".dcm"))
                    with open(os.path.join(dest_dir, newpath), 'wb') as fout:
                        fout.write(os.urandom(fileSizeInBytes)) 
                # modify contents
                elif option == 2:
                    with open(dest_path, 'wb') as fout:
                        fout.write(os.urandom(fileSizeInBytes)) 
                # symbolic link
                elif option == 3:
                    # if on windows, this will fail
                    # if os.name == 'nt':
                    shutil.copy(origpath, dest_path)
                    # else:
                    # os.symlink(os.path.abspath(origpath), os.path.abspath(dest_path))
                    
                    
            option = random.randint(0,3)
            if option == 1:  # add some more folders.  1/4 change
                
                # create a new subdirectory.
                subdir = str(random.randint(10000,20000)) if modality == "Images" else os.path.join(str(random.randint(10000,20000)),str(random.randint(10000,20000)))
                personwfdir = os.path.join(personwfdir_2, subdir)
                os.makedirs(personwfdir, exist_ok=True)
                    
                # need: files corresponding to a pt
                intervalpts = [random.randint(unix0_2023,unix0_2024) for i in range(numfiles*2)]
                intervalpts.sort()
                print(intervalpts)
                
                for filenum in range(numfiles):
                    interval_st = intervalpts[2*(filenum)]
                    interval_end = intervalpts[2*(filenum)+1]
                    interval_dur = interval_end - interval_st
                    datestr = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                    fileSizeInBytes = 1024
                    # outputfilename = str(id) +  "_" + str(random.randint(10000,20000)) + "_"
                    outputfilename = str(id) +  "_" + datestr + "_" + ((str(interval_dur) + ".dat") if modality == "Waveforms" else ("CT_01_" + str(filenum) + ".dcm")) 
                    outputfilepath = os.path.join(personwfdir, outputfilename)
                    with open(outputfilepath, 'wb') as fout:
                        fout.write(os.urandom(fileSizeInBytes)) 

    # OMOP directory - all updated
    personwfdir = os.path.join(secpushdir, "OMOP")
    os.makedirs(personwfdir, exist_ok=True)
    print("Updating OMOP")
    for filenum in range(14):
        fileSizeInBytes = 1024
        outputfilename = str(filenum) + ".csv"
        outputfilepath = os.path.join(personwfdir, outputfilename)
        with open(outputfilepath, 'wb') as fout:
            fout.write(os.urandom(fileSizeInBytes)) 