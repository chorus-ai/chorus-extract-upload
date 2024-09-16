#!/bin/bash
rm journal.db

# test configuration
# phase 1:  with firstsnapshot data
# update journal
# generate file list
# with local journal and upload to central azcli
# generate file list
# with local journal and upload to central azcli - AMENDING
# with local journal and upload to central built in - AMENDING

# update journal with second snapshot data
# with local journal and upload to central using built in
# verify version


# phase 2:  cloud journal with firstsnapshot data
# update journal
# generate file list
# with cloud journal and upload to central azcli
# generate file list
# with cloud journal and upload to central azcli - AMENDING
# with cloud journal and upload to central built in - AMENDING

# update journal with second snapshot data
# with cloud journal and upload to central using builcloud
# verify version

# phase 3: mixture of local and cloud
# checkout cloud journal
# update journal with local
# generate file list
# with local journal and upload to central azcli
# generate file list
# with local journal and upload to central azcli - AMENDING
# checkin journal
# generate file list central
# with central journal and upload to central azcli - AMENDING
# with cloud journal upload to central built in - AMENDING
# verify version

account="cs210032003ae243f15"
container="test"
sas="sp=racwdl&st=2024-09-05T14:27:06Z&se=2024-10-05T22:27:06Z&spr=https&sv=2022-11-02&sr=c&sig=k5NAbtzExdld%2ByHL6uzSGoUYyJD7frpYNR3CHob9Jyk%3D"

# clean up
az storage blob delete-batch --account-name ${account} --source ${container} --sas-token ${sas} --pattern "journal.db*"
az storage blob delete-batch --account-name ${account} --source ${container} --sas-token ${sas} --pattern "2024*"


# test configuration
echo TEST phase 1:  with firstsnapshot data
rm journal.db
config=tests/configlocal.toml
azcli=TestData/azcli1.sh
azcli2=TestData/azcli2.sh

echo TEST update journal
echo python chorus_upload -v -c ${config} journal update --modalities OMOP,Waveforms
python chorus_upload -v -c ${config} journal update --modalities OMOP,Waveforms
cp journal.db TestData/journal1.1.db

echo TEST generate file list
echo python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
cat ${azcli}

echo TEST with local journal and upload to central azcli
chmod a+x ${azcli}
# run the azcli script
eval ${azcli}

# database should have partially uploaded data.
# wait for user input
cp journal.db TestData/journal1.2.db

echo python chorus_upload -v -c ${config} journal update --modalities Images
python chorus_upload -v -c ${config} journal update --modalities Images
cp journal.db TestData/journal1.3.db


echo TEST generate file list again 
echo python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli2} --output-type azcli
python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli2} --output-type azcli
cat ${azcli2}

echo TEST with local journal and upload to central azcli - AMENDING
chmod a+x ${azcli2}
# run the azcli script
eval ${azcli2}
cp journal.db TestData/journal1.4.db

echo TEST with local journal and upload to central built in - AMENDING
echo python chorus_upload -v -c ${config} file upload --modalities OMOP,Waveforms
python chorus_upload -v -c ${config} file upload --modalities OMOP,Waveforms
cp journal.db TestData/journal1.5.db


echo TEST update journal with second snapshot data
echo python chorus_upload -v -c ${config} journal update --modalities Images
python chorus_upload -v -c ${config} journal update --modalities Images
cp journal.db TestData/journal1.6.db


echo TEST with local journal and upload to central using built in
echo python chorus_upload -v -c ${config} file upload --modalities Images
python chorus_upload -v -c ${config} file upload --modalities Images
cp journal.db TestData/journal1.7.db


echo TEST verify the version
echo python chorus_upload -v -c ${config} file verify --modalities OMOP
python chorus_upload -v -c ${config} file verify --modalities OMOP

# ========================
echo TEST phase 2:  cloud journal with firstsnapshot data
rm journal.db
az storage blob delete-batch --account-name ${account} --source ${container} --sas-token ${sas} --pattern "journal.db*"
config=tests/config.toml
azcli=TestData/azcli3.sh
azcli2=TestData/azcli4.sh


echo TEST update journal
echo python chorus_upload -v -c ${config} journal update --modalities OMOP,Waveforms
python chorus_upload -v -c ${config} journal update --modalities OMOP,Waveforms
cp journal.db TestData/journal2.1.db

echo TEST generate file list
echo python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
cat ${azcli}

echo TEST with cloud journal and upload to central azcli
chmod a+x ${azcli}
# run the azcli script
eval ${azcli}
cp journal.db TestData/journal2.2.db

echo python chorus_upload -v -c ${config} journal update --modalities Images
python chorus_upload -v -c ${config} journal update --modalities Images
cp journal.db TestData/journal2.3.db


echo TEST generate file list again
echo python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli2} --output-type azcli
python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli2} --output-type azcli
cat ${azcli2}

echo TEST with cloud journal and upload to central azcli - AMENDING
chmod a+x ${azcli2}
# run the azcli script
eval ${azcli2}
cp journal.db TestData/journal2.4.db

echo TEST with cloud journal and upload to central built in - AMENDING
echo python chorus_upload -v -c ${config} file upload --modalities OMOP,Waveforms
python chorus_upload -v -c ${config} file upload --modalities OMOP,Waveforms
cp journal.db TestData/journal2.5.db


echo TEST update journal with second snapshot data
echo python chorus_upload -v -c ${config} journal update --modalities Images
python chorus_upload -v -c ${config} journal update --modalities Images
cp journal.db TestData/journal2.6.db


echo TEST with cloud journal and upload to central using builcloud
echo python chorus_upload -v -c ${config} file upload --modalities Images
python chorus_upload -v -c ${config} file upload --modalities Images
cp journal.db TestData/journal2.7.db

echo TEST verify the version
echo python chorus_upload -v -c ${config} file verify --modalities OMOP
python chorus_upload -v -c ${config} file verify --modalities OMOP


# ========================
echo TEST phase 3: mixture of local and cloud
rm journal.db
az storage blob delete-batch --account-name ${account} --source ${container} --sas-token ${sas} --pattern "journal.db*"
config=tests/config.toml
configlocal=tests/configlocal.toml  # this should reference localjournal
azcli=TestData/azcli5.sh
azcli2=TestData/azcli6.sh
localjournal=journal.db

echo TEST checkout cloud journal, update, then check in.
echo python chorus_upload -v -c ${config} journal checkout --local-journal ${localjournal}
python chorus_upload -v -c ${config} journal checkout --local-journal ${localjournal}
cp journal.db TestData/journal3.1.db

echo python chorus_upload -v -c ${configlocal} journal update --modalities OMOP,Waveforms
python chorus_upload -v -c ${configlocal} journal update --modalities OMOP,Waveforms
cp journal.db TestData/journal3.2.db

echo python chorus_upload -v -c ${config} journal checkin --local-journal ${localjournal}
python chorus_upload -v -c ${config} journal checkin --local-journal ${localjournal}

echo TEST checkout cloud journal 
echo python chorus_upload -v -c ${config} journal checkout --local-journal ${localjournal}
python chorus_upload -v -c ${config} journal checkout --local-journal ${localjournal}
cp journal.db TestData/journal3.3.db


echo TEST generate file list
echo python chorus_upload -v -c ${configlocal} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
python chorus_upload -v -c ${configlocal} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
cat ${azcli}

echo TEST with local journal and upload to central azcli
chmod a+x ${azcli}
# run the azcli script
eval ${azcli}
cp journal.db TestData/journal3.4.db


echo TEST generate file list
echo python chorus_upload -v -c ${configlocal} file list --modalities OMOP,Waveforms --output-file ${azcli2} --output-type azcli
python chorus_upload -v -c ${configlocal} file list --modalities OMOP,Waveforms --output-file ${azcli2} --output-type azcli
cat ${azcli2}

echo TEST with local journal and upload to central azcli - AMENDING
chmod a+x ${azcli2}
# run the azcli script
eval ${azcli2}
cp journal.db TestData/journal3.5.db


echo TEST checkin journal
echo python chorus_upload -v -c ${config} journal checkin --local-journal ${localjournal}
python chorus_upload -v -c ${config} journal checkin --local-journal ${localjournal}


echo TEST generate file list central
echo python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
python chorus_upload -v -c ${config} file list --modalities OMOP,Waveforms --output-file ${azcli} --output-type azcli
cat ${azcli}

echo TEST with central journal and upload to central azcli - AMENDING
chmod a+x ${azcli}
# run the azcli script
eval ${azcli}
cp journal.db TestData/journal3.6.db


echo TEST with cloud journal upload to central built in - AMENDING
echo python chorus_upload -v -c ${config} file upload --modalities OMOP,Waveforms
python chorus_upload -v -c ${config} file upload --modalities OMOP,Waveforms
cp journal.db TestData/journal3.7.db

echo TEST verify the version
echo python chorus_upload -v -c ${config} file verify --modalities OMOP
python chorus_upload -v -c ${config} file verify --modalities OMOP
