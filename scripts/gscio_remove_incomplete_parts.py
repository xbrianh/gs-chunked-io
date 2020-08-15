#!/usr/bin/env python
"""
This script deletes upload parts. This should not be executed on buckets with uploads in progress without passing in
the `upload-id` of an aborted upload.
"""
import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud.storage import Client

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from gs_chunked_io.writer import remove_parts


parser = argparse.ArgumentParser()
parser.add_argument("--bucket", "-b", type=str, required=True,
                    help="Name of target bucket.")
parser.add_argument("--billing-project", "-p", type=str, default=None, required=False,
                    help="Google billing project to associate with Google Storage requests.")
parser.add_argument("--upload-id", "-u", type=str, default=None, required=False,
                    help="Remove only parts for this upload. If ommitted, remove parts for all uplaods.")
args = parser.parse_args()

kwargs = dict()
if args.billing_project is not None:
    kwargs['user_project'] = args.billing_project
bucket = Client().bucket(args.bucket, **kwargs)
remove_parts(bucket, upload_id=args.upload_id)
