import os
import csv
import subprocess
import shutil
import sys
import datetime
import re
from glob import glob
from collections import defaultdict
from pathlib import Path

# Configuration
EXIFTOOL = '/opt/homebrew/bin/exiftool'
WORKDIR = os.getcwd()
CSV_FILE = os.path.join(WORKDIR, 'metadata.csv')
DIR_DONE = os.path.join(WORKDIR, 'Done')
DIR_FAILED = os.path.join(WORKDIR, 'Failed')
DIR_NOMATCH = os.path.join(WORKDIR, 'NoMatch')
DIR_AMBIG = os.path.join(WORKDIR, 'Ambiguous')

# Similarity thresholds
SIZE_TOLERANCE = 1024  # 1KB tolerance for file size
DATE_TOLERANCE = 300   # 5 minutes tolerance for timestamps

def ensure_dirs():
    """Create necessary directories if they don't exist."""
    for d in (DIR_DONE, DIR_FAILED, DIR_NOMATCH, DIR_AMBIG):
        os.makedirs(d, exist_ok=True)

def run_exiftool(args):
    """Run exiftool with the given arguments and return the result."""
    proc = subprocess.run([EXIFTOOL, *args],
                         capture_output=True, text=True)
    if proc.stderr.strip():
        print("    ⚠ exiftool stderr:", proc.stderr.strip(), file=sys.stderr)
    return proc

def get_exif_data(img_path):
    """Get all EXIF data for an image as a dictionary."""
    try:
        proc = run_exiftool(['-j', img_path])
        if not proc.stdout.strip():
            return {}
        
        # Parse the JSON output
        import json
        data = json.loads(proc.stdout)
        return data[0] if isinstance(data, list) and len(data) > 0 else {}
    except Exception as e:
        print(f"Error parsing EXIF data for {img_path}: {e}", file=sys.stderr)
        return {}

def get_img_metadata(img_path):
    """Extract key metadata from an image file."""
    try:
        exif = get_exif_data(img_path)
        if not exif:
            # Fallback to basic file info if EXIF parsing fails
            return {
                'path': img_path,
                'filename': os.path.basename(img_path),
                'size': os.path.getsize(img_path),
                'raw_filename': '',
                'width': 0,
                'height': 0,
                'create_date': '',
                'date_time_original': '',
                'modify_date': str(datetime.datetime.fromtimestamp(os.path.getmtime(img_path))),
                'camera_model': '',
                'lens': ''
            }
            
        # Safely extract values with defaults
        return {
            'path': img_path,
            'filename': os.path.basename(img_path),
            'size': os.path.getsize(img_path),
            'raw_filename': exif.get('RawFileName', ''),
            'width': int(exif.get('ImageWidth', 0)),
            'height': int(exif.get('ImageHeight', 0)),
            'create_date': exif.get('CreateDate', ''),
            'date_time_original': exif.get('DateTimeOriginal', ''),
            'modify_date': exif.get('FileModifyDate', ''),
            'camera_model': exif.get('Model', ''),
            'lens': exif.get('LensModel', '')
        }
    except Exception as e:
        print(f"Error reading metadata for {img_path}: {e}", file=sys.stderr)
        # Return minimal metadata with just the path and filename
        return {
            'path': img_path,
            'filename': os.path.basename(img_path),
            'size': os.path.getsize(img_path),
            'raw_filename': '',
            'width': 0,
            'height': 0,
            'create_date': '',
            'date_time_original': '',
            'modify_date': str(datetime.datetime.fromtimestamp(os.path.getmtime(img_path))),
            'camera_model': '',
            'lens': ''
        }

def build_img_index():
    """Build an index of all images with their metadata."""
    print("🔍 Indexing images...")
    images = []
    for ext in ('*.jpg', '*.jpeg', '*.JPG', '*.JPEG', '*.png', '*.PNG'):
        for img_path in glob(os.path.join(WORKDIR, ext)):
            if os.path.isfile(img_path) and not any(
                d in img_path for d in (DIR_DONE, DIR_FAILED, DIR_NOMATCH, DIR_AMBIG)
            ):
                meta = get_img_metadata(img_path)
                if meta:
                    images.append(meta)
                    print(f" • Indexed: {meta['filename']}")
    return images

def find_best_match(csv_row, images):
    """Find the best matching image for the given CSV row."""
    if not images:
        return None
        
    filename = csv_row['Filename'].strip()
    csv_size = int(csv_row.get('File Size', 0) or 0)
    
    # Normalize both filenames for comparison
    base_filename = os.path.splitext(filename)[0].lower()
    
    # Extract number sequences from filenames for matching
    def get_numbers(s):
        return ''.join(c for c in s if c.isdigit())
    
    csv_numbers = get_numbers(filename)
    
    # Calculate size tolerance (5% of file size or 100KB, whichever is smaller)
    size_tolerance = min(csv_size * 0.05, 100 * 1024) if csv_size > 0 else 0
    
    # Try different matching strategies in order of confidence
    strategies = [
        # 1. Match by RawFileName (no extension, case insensitive)
        lambda img: img.get('raw_filename') and os.path.splitext(img['raw_filename'])[0].lower() == base_filename,
        # 2. Exact size match (within 1KB)
        lambda img: csv_size > 0 and abs(img['size'] - csv_size) <= 1024,  # Within 1KB
        # 3. Size match with tolerance (5% or 100KB)
        lambda img: csv_size > 0 and abs(img['size'] - csv_size) <= size_tolerance,
        # 4. Exact filename match (case insensitive)
        lambda img: img['filename'].lower() == filename.lower(),
        # 5. Base filename (without extension) matches exactly
        lambda img: os.path.splitext(img['filename'])[0].lower() == base_filename,
        # 6. For JHR files, match by number sequence to "Jobb bonus i Lillestrøm kommune" files
        lambda img: (filename.startswith('JHR') and 
                    img['filename'].startswith('Jobb bonus i Lillestrøm kommune') and
                    get_numbers(img['filename']) == csv_numbers),
        # 7. For SAL- files, match by number to "Overlege Jacob Dag Berild" files
        lambda img: (filename.startswith('SAL-') and 
                    img['filename'].startswith('Overlege Jacob Dag Berild') and
                    get_numbers(img['filename']) == csv_numbers),
        # 8. Match by number sequence only if we have numbers
        lambda img: (csv_numbers and 
                    get_numbers(img['filename']) == csv_numbers),
        # 9. Filename contains CSV filename (without extension) or vice versa
        lambda img: (base_filename in os.path.splitext(img['filename'])[0].lower() or 
                    os.path.splitext(img['filename'])[0].lower() in base_filename),
    ]
    
    # Apply each strategy until we find matches
    for strategy in strategies:
        matches = [img for img in images if strategy(img)]
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            # If multiple matches, try to disambiguate by size if available
            if csv_size > 0:
                size_matches = [img for img in matches 
                              if abs(img['size'] - csv_size) <= size_tolerance]
                if len(size_matches) == 1:
                    return size_matches[0]
            return disambiguate_matches(csv_row, matches)
    
    # If no matches found with strategies, try to find by partial match in filename
    for img in images:
        img_base = os.path.splitext(img['filename'])[0].lower()
        
        # If sizes are very close (within 1KB), it's likely a match
        if csv_size > 0 and abs(img['size'] - csv_size) <= 1024:
            print(f"  ℹ Found match by file size: {img['filename']} (size: {img['size']:,} bytes)")
            return img
            
        # Try to match by any common words
        csv_words = set(base_filename.split())
        img_words = set(img_base.split())
        common_words = csv_words.intersection(img_words)
        
        # If we have at least 2 common words or one long common word
        if (len(common_words) >= 2 or 
            any(len(word) > 5 for word in common_words)):
            print(f"  ℹ Found potential match by common words: {common_words}")
            return img
    
    return None

def disambiguate_matches(csv_row, matches):
    """Disambiguate between multiple potential matches."""
    if not matches:
        return None
        
    if len(matches) == 1:
        return matches[0]
    
    # Try to find the best match using additional criteria
    csv_size = int(csv_row.get('File Size', 0) or 0)
    
    # 1. Closest file size
    size_diff = [abs(m['size'] - csv_size) for m in matches]
    min_size_diff = min(size_diff)
    size_matches = [m for m, diff in zip(matches, size_diff) 
                   if diff == min_size_diff]
    
    if len(size_matches) == 1:
        return size_matches[0]
    
    # 2. Most recent modification date (if available)
    try:
        csv_date = datetime.datetime.strptime(
            csv_row.get('Published Date', ''), 
            "%Y-%m-%d %H:%M:%S"
        )
        date_diffs = []
        for m in matches:
            try:
                img_date = datetime.datetime.strptime(
                    m['modify_date'].split('.')[0], 
                    "%Y:%m:%d %H:%M:%S"
                )
                date_diffs.append(abs((img_date - csv_date).total_seconds()))
            except:
                date_diffs.append(float('inf'))
        
        min_date_diff = min(date_diffs)
        if min_date_diff < DATE_TOLERANCE:
            date_matches = [m for m, diff in zip(matches, date_diffs) 
                          if diff == min_date_diff]
            if len(date_matches) == 1:
                return date_matches[0]
    except:
        pass
    
    # 3. If still ambiguous, return None and let the user decide
    return None

def apply_iptc_metadata(img_path, csv_row, dry_run=False):
    """Apply IPTC metadata to the image using exiftool."""
    try:
        if not os.path.exists(img_path):
            print(f"  ⚠ File not found: {img_path}")
            return False
            
        # Prepare metadata - only overwrite specified IPTC fields
        ext = os.path.splitext(img_path)[1].lower()
        if ext == '.png':
            # Use XMP tags for PNG since IPTC is unsupported
            metadata = {
                'XMP:Title':           csv_row.get('Title', '').strip(),
                'XMP:Description':     csv_row.get('Description', '').strip(),
                'XMP:Subject':         csv_row.get('Tags', '').strip(),
                'XMP:Rights':          csv_row.get('Kreditering', '').strip(),
                'XMP:PersonInImage':   csv_row.get('Personer i bildet', '').strip()
            }
        else:
            metadata = {
                'IPTC:Headline': csv_row.get('Title', '').strip(),
                'IPTC:Caption-Abstract': csv_row.get('Description', '').strip(),
                'IPTC:CopyrightNotice': csv_row.get('Kreditering', '').strip(),
                'IPTC:Keywords': csv_row.get('Tags', '').strip(),
                'XMP:PersonInImage': csv_row.get('Personer i bildet', '').strip()
            }
        
        # Build exiftool command
        cmd = [EXIFTOOL, '-overwrite_original', '-charset', 'IPTC=UTF8', '-charset', 'XMP=UTF8']
        for tag, value in metadata.items():
            if value:  # Only add non-empty tags
                cmd.extend([f'-{tag}={value}'])
        cmd.append(img_path)
        
        if dry_run:
            print(f"  [DRY RUN] Would apply metadata to {os.path.basename(img_path)}")
            for tag, value in metadata.items():
                if value:
                    print(f"    Set {tag}: {value[:100]}{'...' if len(value) > 100 else ''}")
            return True
            
        print(f"  Applying metadata to {os.path.basename(img_path)}")
        
        # Run exiftool
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"  ⚠ exiftool stderr: {result.stderr.strip()}")
            print(f"  ❌ Failed to apply metadata: {result.stderr.strip()}")
            return False
            
        return True
        
    except Exception as e:
        print(f"  ❌ Error processing {img_path}: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Apply IPTC metadata to images based on CSV data.')
    parser.add_argument('--dry-run', action='store_true', help='Run without making any changes')
    args = parser.parse_args()
    
    print("🚀 Starting metadata application..." + ("\n🔍 DRY RUN MODE - No files will be modified" if args.dry_run else ""))
    
    # Create output directories if they don't exist
    for directory in [DIR_DONE, DIR_FAILED, DIR_NOMATCH, DIR_AMBIG]:
        os.makedirs(directory, exist_ok=True)
    
    # Index all images
    print("🔍 Indexing images...")
    images = build_img_index()
    
    if not images:
        print("❌ No images found in the directory.")
        return
    
    # Process CSV file
    print("\n📄 Processing CSV file...\n")
    csv_file = os.path.join(os.getcwd(), 'metadata.csv')
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    processed = 0
    success = 0
    failed = 0
    no_match = 0
    ambiguous = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            processed += 1
            filename = row['Filename'].strip()
            print(f"\n📄 Processing: {filename}")
            
            # Find best match
            match = find_best_match(row, images)
            
            if not match:
                print(f"  ❌ No match found")
                no_match += 1
                continue
                
            if match == 'ambiguous':
                print(f"  ⚠ Multiple possible matches found")
                ambiguous += 1
                continue
                
            # Apply metadata
            print(f"  ✅ Match found: {match['filename']}")
            
            # Skip if the file has already been processed
            dest_path = os.path.join(DIR_DONE, match['filename'])
            if os.path.exists(dest_path):
                print(f"  ℹ Already processed, skipping: {match['filename']}")
                success += 1
                continue
                
            if apply_iptc_metadata(match['path'], row, dry_run=args.dry_run):
                success += 1
                # Move to done directory if not dry run
                if not args.dry_run:
                    try:
                        shutil.move(match['path'], dest_path)
                    except FileNotFoundError:
                        print(f"  ⚠ File not found, may have been moved already: {match['path']}")
                    except Exception as e:
                        print(f"  ❌ Error moving file: {str(e)}")
                        failed += 1
            else:
                failed += 1
                # Move to failed directory if not dry run
                if not args.dry_run:
                    try:
                        failed_path = os.path.join(DIR_FAILED, match['filename'])
                        shutil.move(match['path'], failed_path)
                    except Exception as e:
                        print(f"  ❌ Error moving failed file: {str(e)}")
    
    # Print summary
    print("\n📊 Processing Summary:")
    print(f"  Total entries: {processed}")
    print(f"  Successfully matched: {success}")
    print(f"  Failed: {failed}")
    print(f"  No match found: {no_match}")
    print(f"  Ambiguous matches: {ambiguous}")
    
    if args.dry_run:
        print("\n💡 This was a dry run. No files were modified. Use --dry-run to test without making changes.")
    elif failed > 0:
        print(f"\n⚠ Some files failed to process. Check the logs above for details.")
        print(f"  Failed files were moved to: {DIR_FAILED}")
    
    if no_match > 0:
        print(f"\nℹ Could not find matches for {no_match} entries. Check the 'NoMatch' directory.")

if __name__ == "__main__":
    import argparse
    main()