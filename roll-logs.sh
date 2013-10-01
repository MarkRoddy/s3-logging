#!/bin/bash

set -e



if [ 3 != $# ]; then
    echo "usage: $0 log-bucket site-reverse-dns yyyy-mm-dd";
    exit 1;
fi

LOGBUCKET="$1"
DOMAIN="$2"
LOGDATE="$3"
echo "Rolling logs in bucket $LOGBUCKET from site $DOMAIN on $LOGDATE"

# Directory where we'll do all of our work
TMPDIR=/tmp/s3-log-roll-$DOMAIN-$LOGDATE

# Directory where the log files get downloaded
LOGDIR="$TMPDIR/logs"
mkdir -p "$LOGDIR"

# Directory where we store the combined log files
COMBLOGDIR="$TMPDIR/combined-logs"
mkdir -p "$COMBLOGDIR"
COMBINEDFILE="$COMBLOGDIR/$LOGDATE.log"
COMPRESSEDFILE="$COMBLOGDIR/$LOGDATE.log.gz"

# s3 url of the combined/compressed file we'll be uploading
S3COMBINEDFILE=s3://$LOGBUCKET/combined/$DOMAIN/$DOMAIN-$LOGDATE.log.gz

# File to pump results of the `s3cmd ls` command to. As soon as that
# process is done we are going to rename the file to indicate that the
# process completed successfully to achieve idempotency.  We do the same
# with a downloaded file to indicate that we finished the download phase.
LISTING_FILE=$TMPDIR/files.listing
LISTED_FILE=$TMPDIR/files.listed
DOWNLOADED_FILE=$TMPDIR/files.downloaded
DELETED_FILE=$TMPDIR/files.deleted

if [ ! -f "$LISTED_FILE" ]; then
    s3cmd ls s3://$LOGBUCKET/$DOMAIN/$LOGDATE* > $LISTING_FILE
    mv "$LISTING_FILE" "$LISTED_FILE"
fi

if [ ! -f $DOWNLOADED_FILE ]; then
    for S3URL in `
    cat $LISTED_FILE | \
        grep -v "^Bucket " | \
        cut -d ':' -f 3 | \
        sed 's/^/s3:/' `; do

        # Pull out the date of the logfile and create a directory structure containing
        # it. This is so that we don't inadvertantly pull too many files in to the same
        # directory which some file systems can't handle. So we take:
        #     s3://LOGBUCKET/PREFIX/2013-08-28-22-23-25-972D8E96B22FF625
        # And download it into:
        #     LOGDIR/2013/08/28/22/23/2013-08-28-22-23-25-972D8E96B22FF625
        BARENAME=`basename $S3URL`;
        DATETIME=`echo $BARENAME| cut -d '-' -f 1,2,3,4,5`
        DATEDDIR=$LOGDIR/`echo $DATETIME|sed 's/-/\//g'`
        mkdir -p $DATEDDIR; 
        DESTFILE="$DATEDDIR/$BARENAME";
        if [ ! -f "$DESTFILE" ]; then
            s3cmd get $S3URL $DESTFILE;
        fi
    done
    touch $DOWNLOADED_FILE;
fi



# Now we do the combine, we do this if the compression step hasn't 
# finished as there's no easy way to see if it was interrupted midstream.
if [ ! -f "$COMPRESSEDFILE" ]; then
    rm -f "$COMBINEDFILE";
    for LEAFDIR in `find $LOGDIR -type d -links 2`; do
        # we're doing this by find all leaf directories
        # and using shell globbing from them to append to the final file. This
        # uses much less resources than catting each file individually but also 
        # avoids the too many files to glob issue if we tried to handle them all
        # at once.
        cat $LEAFDIR/* >> "$COMBINEDFILE";
    done
    gzip "$COMBINEDFILE";
fi

# Now we upload the combined file back to s3
if [ ! -f "$COMPRESSEDFILE.uploaded" ]; then
    # Since older versions of s3cmd don't support the 'test' command
    # we drop a marker file to indicate that the upload completed
    s3cmd put "$COMPRESSEDFILE" "$S3COMBINEDFILE";
    touch "$COMPRESSEDFILE.uploaded";
fi

# And we kill the individual log files on s3 that we just combined
if [ ! -f "$DELETED_FILE" ]; then
    # Older versions of s3cmd don't accept wild cards for the del command
    # so we delete each file individually. Sorry.
    for S3URL in `
    cat $LISTED_FILE | \
        grep -v "^Bucket " | \
        cut -d ':' -f 3 | \
        sed 's/^/s3:/' `; do
        s3cmd del $S3URL;
    done
    touch "$DELETED_FILE"
fi
