import os
from datetime import datetime
import random
import shutil
import numpy as np

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

    numfiles = 5

    unix0_2023 = 1672549200
    unix0_2024 = 1704085200

    for id in personids:
        persondir = os.path.join(initpushdir, str(id))
        if not os.path.exists(persondir):
            os.mkdir(persondir)
        personwfdir = os.path.join(persondir, "Waveforms")
        if not os.path.exists(personwfdir):
            os.mkdir(personwfdir)
            
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
            outputfilename = str(id) +  "_" + datestr + "_" + str(interval_dur)
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

    personids = [int(i) for i in os.listdir(initpushdir)]

    for id in personids:
        personwfdir_1 = os.path.join(initpushdir, str(id),"Waveforms")
        persondir_2 = os.path.join(secpushdir, str(id))
        if not os.path.exists(persondir_2):
            os.mkdir(persondir_2)
        personwfdir_2 = os.path.join(persondir_2, "Waveforms")
        if not os.path.exists(personwfdir_2):
            os.mkdir(personwfdir_2)
        # need: files corresponding to a pt
        for curfile in os.listdir(personwfdir_1):
            # do nothing
            # delete and add a new file
            # mod
            option = random.randint(0,2)
            origpath = os.path.join(personwfdir_1,curfile)
            # copy as is
            if option == 0:
                shutil.copy(origpath, personwfdir_2)
            # delete this file and add a file in place
            if option == 1:
                interval_st = random.randint(unix0_2023,unix0_2024)
                interval_end = random.randint(unix0_2023,unix0_2024)
                if(interval_st > interval_end):
                    temp = interval_st
                    interval_st = interval_end
                    interval_end = temp
                interval_dur = interval_end - interval_st
                datestr = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                newpath = str(id) +  "_" + datestr + "_" + str(interval_dur)
                shutil.copy(origpath, os.path.join(personwfdir_2,newpath))
            # modify contents
            if option == 2:
                newpath = os.path.join(personwfdir_2, curfile)
                with open(newpath, 'wb') as fout:
                    fout.write(os.urandom(fileSizeInBytes)) 
            # # delete
            # if option == 3:
            #     pass