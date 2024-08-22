#!/bin/bash
rm journal.db
# rm journal2.db

echo "UPDATE 1 - waveform only"
echo python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config.toml update --modalities Waveforms
python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config.toml update --modalities Waveforms

echo "LIST 1 - waveform only"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml select --modalities Waveforms
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml select --modalities Waveforms

echo "UPDATE 1.1 - images only"
echo python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config.toml update --modalities Images
python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config.toml update --modalities Images

echo "LIST 1.1 - images only"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml select --modalities Images
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml select --modalities Images

echo "UPLOAD 1 - waveforms and images"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml upload
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml upload

echo "verify 1"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml verify
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml verify

echo "UPDATE 1.2 check OMOP and Images"
echo python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config.toml update --modalities OMOP,Images
python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config.toml update --modalities OMOP,Images

echo "LIST 1.2  - should have OMOP only"
echo python upload_manifest/generate_manifes -c upload_manifest/generate_manifest/config.toml select
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config.toml select

echo "UPDATE 2 - second upload OMOP and Images"
echo python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml update --modalities Images,OMOP
python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml update --modalities Images,OMOP

echo "LIST 2 - should be OMOP and images"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml select --modalities Images,OMOP
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml select --modalities Images,OMOP

echo "UPDATE 2.1 - should not change.   OMOP and Images"
echo python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml update --modalities Images,OMOP
python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml update  --modalities Images,OMOP

echo "LIST 2.1 - should not change,  OMOP and images"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml select --modalities Images,OMOP
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml select --modalities Images,OMOP

echo "UPLOAD 2 - amend.  verify will only verify uploads."
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml upload --amend --modalities Images,OMOP
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml upload --amend --modalities Images,OMOP

echo "verify 2.  this can verify more than the amended - because the amended version is verified in total."
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml verify --modalities Images,OMOP
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml verify --modalities Images,OMOP

echo "UPDATE 2.2 - new wavforms"
echo python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml update
python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml update

echo "LIST 2.2  - new waveforms"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml select
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml select

echo "UPLOAD 2.2 - should add new waveforms"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml upload
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml upload

echo "verify 2.2"
echo python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml verify
python upload_manifest/generate_manifest -c upload_manifest/generate_manifest/config2.toml verify


python upload_manifest/generate_manifest -v -c upload_manifest/generate_manifest/config2.toml history
