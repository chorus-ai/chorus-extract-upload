import os
import shutil
import time

from generate_manifest import gen_manifest, update_manifest

if __name__ == "__main__":
    if os.path.exists("journal.db"):
        os.remove("journal.db")
    gen_manifest("TestData/SiteFolder_FirstSnapshot")
    time.sleep(2)
    update_manifest("TestData/SiteFolder_SecondSnapshot")
    