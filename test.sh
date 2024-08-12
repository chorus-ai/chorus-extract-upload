#!/bin/bash
rm journal.db
# rm journal2.db

account_name="blah"
account_key="blah"
sas="blahblah"

echo "UPDATE 1 - waveform only"
echo python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --modalities Waveforms
python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --modalities Waveforms

echo "LIST 1 - waveform only"
echo python upload_manifest/generate_manifest -v -m journal.db select --modalities Waveforms
python upload_manifest/generate_manifest -v -m journal.db select --modalities Waveforms

echo "UPDATE 1.1 - images only"
echo python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --modalities Images
python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --modalities Images

echo "LIST 1.1 - images only"
echo python upload_manifest/generate_manifest -v -m journal.db select --modalities Images
python upload_manifest/generate_manifest -v -m journal.db select --modalities Images

echo "UPLOAD 1 - waveforms and images"
echo python upload_manifest/generate_manifest -v -m journal.db upload --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --central-path az://test/ --central-azure-account-name $account_name --central-azure-sas-token $sas
python upload_manifest/generate_manifest -v -m journal.db upload --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --central-path az://test/ --central-azure-account-name $account_name --central-azure-sas-token $sas

echo "verify 1"
echo python upload_manifest/generate_manifest -v -m journal.db verify --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key
python upload_manifest/generate_manifest -v -m journal.db verify --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key

echo "UPDATE 1.2 check OMOP and Images"
echo python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --modalities OMOP,Images
python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_FirstSnapshot --modalities OMOP,Images

echo "LIST 1.2  - should have OMOP only"
echo python upload_manifest/generate_manifest -v -m journal.db select
python upload_manifest/generate_manifest -v -m journal.db select

echo "UPDATE 2 - second upload OMOP and Images"
echo python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --modalities Images,OMOP
python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --modalities Images,OMOP

echo "LIST 2 - should be OMOP and images"
echo python upload_manifest/generate_manifest -v -m journal.db select --modalities Images,OMOP
python upload_manifest/generate_manifest -v -m journal.db select --modalities Images,OMOP

echo "UPDATE 2.1 - should not change.   OMOP and Images"
echo python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --modalities Images,OMOP
python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot  --modalities Images,OMOP

echo "LIST 2.1 - should not change,  OMOP and images"
echo python upload_manifest/generate_manifest -v -m journal.db select --modalities Images,OMOP
python upload_manifest/generate_manifest -v -m journal.db select --modalities Images,OMOP

echo "UPLOAD 2 - amend.  verify will only verify uploads."
echo python upload_manifest/generate_manifest -v -m journal.db upload --amend --modalities Images,OMOP --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key
python upload_manifest/generate_manifest -v -m journal.db upload --amend --modalities Images,OMOP --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key

echo "verify 2.  this can verify more than the amended - because the amended version is verified in total."
echo python upload_manifest/generate_manifest -v -m journal.db verify --modalities Images,OMOP --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key
python upload_manifest/generate_manifest -v -m journal.db verify --modalities Images,OMOP --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key

echo "UPDATE 2.2 - new wavforms"
echo python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot
python upload_manifest/generate_manifest -v -m journal.db update --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot

echo "LIST 2.2  - new waveforms"
echo python upload_manifest/generate_manifest -v -m journal.db select
python upload_manifest/generate_manifest -v -m journal.db select

echo "UPLOAD 2.2 - should add new waveforms"
echo python upload_manifest/generate_manifest -v -m journal.db upload --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key
python upload_manifest/generate_manifest -v -m journal.db upload --site-path upload_manifest/TestData/SiteFolder_SecondSnapshot --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key

echo "verify 2.2"
echo python upload_manifest/generate_manifest -v -m journal.db verify --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key
python upload_manifest/generate_manifest -v -m journal.db verify --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key


python upload_manifest/generate_manifest -v -m journal.db history

# #--------------
# echo "UPDATE 3"
# echo python upload_manifest/generate_manifest -v -m journal.db update -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default
# python upload_manifest/generate_manifest -v -m journal.db update -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default

# echo "LIST 3"
# echo python upload_manifest/generate_manifest -v -m journal.db select -m journal2.db
# python upload_manifest/generate_manifest -v -m journal.db select -m journal2.db

# echo "UPDATE 3.1 - should not change"
# echo python upload_manifest/generate_manifest -v -m journal.db update -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default
# python upload_manifest/generate_manifest -v -m journal.db update -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default

# echo "LIST 3.1 - should not change"
# echo python upload_manifest/generate_manifest -v -m journal.db select -m journal2.db
# python upload_manifest/generate_manifest -v -m journal.db select -m journal2.db

# echo "UPLOAD 3"
# echo python upload_manifest/generate_manifest -v -m journal.db upload -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key
# python upload_manifest/generate_manifest -v -m journal.db upload -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default --central-path az://test/ --central-azure-account-name $account_name --central-azure-account-key $account_key

# echo "UPDATE 3.2 - should not change"
# echo python upload_manifest/generate_manifest -v -m journal.db update -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default
# python upload_manifest/generate_manifest -v -m journal.db update -m journal2.db --site-path s3://cmat-test/ --site-aws-profile default

# echo "LIST 3.2  - should be empty"
# echo python upload_manifest/generate_manifest -v -m journal.db select -m journal2.db
# python upload_manifest/generate_manifest -v -m journal.db select -m journal2.db