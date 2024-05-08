import os
import shutil

from upload_manifest.create_test_data.generate_file_structure import generateFirstFiles, generateUpdate

if __name__ == "__main__":
    if os.path.exists("TestData"):
        shutil.rmtree("TestData")
    generateFirstFiles()
    generateUpdate()