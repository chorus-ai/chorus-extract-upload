import os
import shutil
import time
import argparse

from generate_manifest import gen_manifest, update_manifest

if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate manifest for a site folder")
    # minimally, need the top level directory
    parser.add_argument("-d", "--data-dir", help="The top level directory of the site data folder", required=True)
    parser.add_argument("-m", "--manifest", help="manifest file (defaults to site_data_fold/journal.db)", required=False)
    
    args = parser.parse_args()
    folder = args.data_dir
    if args.manifest:
        manifest_fn = args.manifest
    else:
        manifest_fn = os.path.join(folder, "journal.db")
    
    print("Manifest to save to: ", manifest_fn)
    if os.path.exists(manifest_fn):
        update_manifest(folder, databasename = manifest_fn)
    else:
        gen_manifest(folder, databasename = manifest_fn)
    
    # if (os.path.exists("journal.db")):
    #     os.remove("journal.db")
    # gen_manifest("TestData/SiteFolder_FirstSnapshot")
    # time.sleep(2)
    # update_manifest("TestData/SiteFolder_SecondSnapshot")
    