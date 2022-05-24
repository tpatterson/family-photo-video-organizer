# Organizes a mess of images and videos into nice subfolders
# for Sharleen

# REQUIRES: python, 
# pip install pillow, ipdb, defusedxml, hachoir

import glob
import sys
import os.path
import pathlib
import ipdb
from PIL import Image, UnidentifiedImageError
import arrow
import datetime
import re
import shutil
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
from hachoir.stream.input import NullStreamError

error_datetime = datetime.datetime.now() + datetime.timedelta(days=1)

# CONFIG STUFF!!! 
SEARCH_DIR = "d:\\CopyOfPhotosFromBackup"
OUTPUT_PATH = "g:\\test_output"
#ignore_younger_than = datetime.datetime(1900, 1, 1)  # Not yet implemented

def get_earliest_datetime(timestamps):
    """
    Given a list of timestamps, each in a different format
    and possibly null, return the earliest of them all
    """
    earliest = None
    for ts in timestamps:
        if not ts:
            continue
        if type(ts) == str:
            if not ts.strip():
                continue
            if ts == '    -  -     :  :  ':
                continue
            if ts.endswith(': 0'):
                ts = ts.replace(': 0', ':00')
            #print(f"Sending to arrow: '{ts}'")
            try:
                #ipdb.set_trace()
                dt = arrow.get(ts).datetime
            except arrow.parser.ParserError:
                print(f"Warning: Arrow parse error in timestamp: '{ts}'")
                continue
            except ValueError as e:
                if 'year 0 is out of range' in str(e):
                    continue
                raise
            except:
                print(f"Uncaught in arrow, timestamp: '{ts}'")
                raise
        elif type(ts) is datetime.datetime:
            dt = ts
        else:
            continue
        #print(ts, dt)
        # timezones mess us up and we don't care
        dt = dt.replace(tzinfo=None)
        if not earliest or dt < earliest:
            earliest = dt
    return earliest

def override_creation_datetime_with_directory_if_needed(full_filename, creation_datetime):
    """
    Given a full file path and a datetime, if the datetime 
    is way different than the datetime in the path 
    we'll overwrite the date portion of it.
    """
    #drive, path_and_file = os.path.splitdrive(full_filename)
    parts = os.path.normpath(full_filename).split(os.path.sep)

    #ipdb.set_trace()
    directory = parts[-2]
    found = re.findall('(\d\d\d\d)\D(\d\d)\D(\d\d)', directory)
    if found and int(found[0][1]) in list(range(1, 13)) and int(found[0][2]) in list(range(0, 24)):
        # The directory starts with a date! See how far off we are
        dt = datetime.datetime(int(found[0][0]), int(found[0][1]), int(found[0][2]))
        if abs(dt - creation_datetime) > datetime.timedelta(days=365):
            creation_datetime = datetime.datetime(
                int(found[0][0]), int(found[0][1]), int(found[0][2]),
                creation_datetime.hour,
                creation_datetime.minute,
                creation_datetime.second,
            )
            
    #print(directory, found, dt)
    return creation_datetime
    

def guess_image_creation_datetime(full_filename):
    """
    Returns our best guess as to the datetime an image was actually taken.
    Uses the earliest of exif data, then falls back to os timestamp
    
    ALSO returns the imagemagick file
    """
    
    try:
        im = Image.open(full_filename)
        old_exif = im._getexif() or {}
        new_exif = im.getexif() or {}
    except OSError:
        print(f"WARNING: OSError when imagemagick tried to open {full_filename}")
        old_exif = {}
        new_exif = {}
    taken_string = ''
    stat = pathlib.Path(full_filename).stat()
    timestamps = (
        old_exif.get(36867, '').replace(':', '-', 2), 
        new_exif.get(306, '').replace(':', '-', 2),
        datetime.datetime.fromtimestamp(stat.st_mtime),
        datetime.datetime.fromtimestamp(stat.st_ctime),
    )
    creation_time = get_earliest_datetime(timestamps)
    #if not creation_time:
    #    creation_time = get_earliest_datetime((
    #        datetime.datetime.fromtimestamp(stat.st_mtime),
    #        datetime.datetime.fromtimestamp(stat.st_ctime),
    #    ))
    if creation_time > error_datetime:
        raise Exception(f"Creation time was in the future: {creation_time}\ntimestamps: {timestamps}")
    
    creation_time = override_creation_datetime_with_directory_if_needed(
            full_filename, creation_time)
    return creation_time


def copy_file_if_needed(full_filename, created_datetime):
    """
    Given a full filename, save it 
    into the correct location first ensuring we're not overwriting
    an existing file of the same name that is larger
    """
    #print(created_datetime, full_filename, im)
    parts = os.path.normpath(full_filename).split(os.path.sep)
    
    ####################################################################
    # Clean up the output filename! Remove nasty characters and prepend 
    # the date and time unless it is already there.
    # This will make it easier for Sharleen to see files organized
    # by time
    datestr = created_datetime.strftime("%Y-%m-%d %H_%M_%S")
    previous_filename = parts[-1]
    # We'll just check if the date is at the beginning while not 
    # being picky about format. To do that we'll squash everything except
    # numbers and see if the beginning matches
    new_filename = re.sub('[^\d\w\-_\ \.]', '_', previous_filename)
    filename_numbers = re.sub('\D', '', new_filename)
    datestr_numbers = re.sub('\D', '', datestr)
    if new_filename[0] in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') and \
            filename_numbers.startswith(datestr_numbers):
        pass
        #print("STARTED WITH!!!", new_filename)
    else:
        # Didn't start with the filename already. So stick it on there.
        new_filename = f"{datestr} {new_filename}"
    # End cleaning up the output filename
    ######################################################################
       
    destination_folder = OUTPUT_PATH + os.path.sep + str(created_datetime.year)
    full_output_filename = os.path.normpath(destination_folder + os.path.sep + new_filename)
    
    # Now see if we will copy it
    should_copy = False
    try:
        #ipdb.set_trace()
        size = os.path.getsize(full_output_filename)
        old_size = os.path.getsize(full_filename)
        if old_size > size:
            should_copy = True
            #print(f"Size says we SHOULD copy ", old_size, size, full_output_filename)
        else:
            pass
            #print(f"Size says we should not copy ", old_size - size)
    except FileNotFoundError:
        should_copy = True
    
    if should_copy:
        try:
            os.makedirs(destination_folder, exist_ok=True)
        except FileExistsError:
            pass
        #print(f"Copying: {full_filename} -> {full_output_filename}")
        
        # This works, but it loses the file creation time. 
        # I tried several ways to keep the creation time and none
        # of them worked. Doesn't look possible. We'll just 
        # keep what we can.
        shutil.copyfile(full_filename, full_output_filename)
        #shutil.copystat(full_filename, full_output_filename)
        os.utime(full_output_filename, (created_datetime.timestamp(), created_datetime.timestamp()))


def process_image(full_filename):
    try:
        try:
            # Read the file
            created_datetime = guess_image_creation_datetime(full_filename)
        except UnidentifiedImageError:
            print(f"Warning, imagemagick could not read image {full_filename}")
            return
        # Save the file
        copy_file_if_needed(full_filename, created_datetime)
    except:
        escaped = full_filename.replace("\\", "\\\\")
        print(f"Uncaught exception! Run this command:\nprocess_image('{escaped}')")
        raise


def guess_video_creation_datetime(full_filename):
    #ipdb.set_trace()
    class InvalidTypeException(RuntimeError):
        pass
    try:
        parser = createParser(full_filename)
        if not parser:
            raise InvalidTypeException("Could not create parser")
        metadata = extractMetadata(parser)
        creation_datetime = metadata.get('creation_date')
        if creation_datetime < datetime.datetime(1990, 1, 1):
            #print("Really old video!", creation_datetime)
            #
            creation_datetime = None
    except NullStreamError:
        creation_datetime = None
    except ValueError as e:
        # Likely the creation_date wasn't in the metadata
        creation_datetime = None
        #print(f"ValueError {e}")
        #ipdb.set_trace()
    except InvalidTypeException:
        # Likely an mp4 which hachoir doesn't work with
        #ipdb.set_trace()
        creation_datetime = None
        

    stat = pathlib.Path(full_filename).stat()
    timestamps = (
        creation_datetime,
        datetime.datetime.fromtimestamp(stat.st_mtime),
        datetime.datetime.fromtimestamp(stat.st_ctime),
    )
    creation_datetime = get_earliest_datetime(timestamps)
    
    creation_datetime = override_creation_datetime_with_directory_if_needed(full_filename, creation_datetime)
    
    return creation_datetime

# Uncomment this with the path to a broken file to debug it!
# Put paths that cause problems here so we can work out bugs in them first:
#ipdb.set_trace()
process_image('d:\\CopyOfPhotosFromBackup\\EXTRA BACKUP DO NOT EDIT\\AllMergedSep2_2020\\Archive DVDs\\Wedding Pics\\IMG_0613.JPG')
process_image('d:\\CopyOfPhotosFromBackup\\EXTRA BACKUP DO NOT EDIT\\AllMergedSep2_2020\\google_play_backup_aug_30_2020\\Takeout\\Google Photos\\1_02-20-2016 14_46\\P1.jpg')
#raise ASDF

# DO VIDEOS
video_patterns = ["*.mov", "*.avi", "*.mp4",]
files_seen_count = 0
for pattern in video_patterns:
    print(f"starting on pattern: {pattern}...")
    for full_filename in glob.glob(f'{SEARCH_DIR}/**/{pattern}', recursive = True):
        if '$RECYCLE.BIN' in full_filename:
            continue
        
        try:
            created_datetime = guess_video_creation_datetime(full_filename)
            copy_file_if_needed(full_filename, created_datetime)
        except:
            print(f"Error parsing video:\n{full_filename}")
            raise
        
        print(created_datetime, full_filename)
        #
        
#sys.exit(1)


# DO IMAGES
image_patterns = ["*.jpg", "*.jpeg"]
files_seen_count = 0
for pattern in image_patterns:
    print(f"starting on pattern: {pattern}...")
    for full_filename in glob.glob(f'{SEARCH_DIR}/**/{pattern}', recursive = True):
        if '$RECYCLE.BIN' in full_filename:
            continue
        files_seen_count += 1
        #if files_seen_count < 57000:
        #    continue
        
        process_image(full_filename)

        #print(full_filename)
        if files_seen_count % 100 == 0:
            print(f"Processed {files_seen_count} files so far. pattern: {pattern}...")



