This directory contains code for the generation and update of a manifest (running file list) for uploading data for CHoRUS sites. It is still early in development. It also attempts to generate and update a corresponding registry of waveform records.

The manifest and registry are generated as sqlite database files - schemas are still under development, but currently use the following:

Manifest (source_files)
- File_id 
- Record_id
- Person_id
- Srcfile (full filename)
- MD5 (computed hash)
- Time_Valid (when the file was added)
- Time_Invalid (when the file was deleted or replaced)

Registry
- Record_id
- Person_id
- Session_id (holdover from MIMIC, currently a hierarchy step between record and person)
- Date
- Time
- Uploaddir (meant for cloud side to track which upload directory a file is in - might remove)
- IsActive (0 or 1, whether the record in question is still mapped to active files)

# Generate Test Data
To generate synthetic test data, use the following command:
```
python create_test_data
```

# Generate Test Manifest with Update
To generate the corresponding manifests for the first upload and the update in the test data, use the following command:
```
python create_test_data
```

# Current assumptions, limitations, and working decisions:
- Tested on Windows
- Waveform only
- For simplicity in generation, 1 file per record - can be extended to 1 dir per record or multiple files per record using a file-record mapping
- Documentation of data version is done with a timestamp at the beginning of the manifest update process
- Entity relationships: many sessions per person, many records per session, many files per record (currently one record per session is used, leaving session in the table in case we want another level in hierarchy)
- Files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension
  - Needed information per file: person_id, start datetime, record grouping (e.g. visit_occurence_id)
