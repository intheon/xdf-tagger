#!/usr/bin/env python
"""Command-line tool for managing metadata tags in XDF files.
Copyright (c) 2019 Syntrogi Inc dba Intheon. All Rights Reserved.

Supports the following operations:
- set: set or override a particular field to a new value
  (will only override first occurrence if multiple in the xml)
- clear: clear (remove) all fields with the given name
- show: show the current values of all current fields with the given nme

"""

import os
import logging
import random
import shutil
import glob
import uuid
import xml.etree.ElementTree as et
import collections
import argparse
import inspect
import struct

logger = logging.getLogger(__name__)

# name/type of the metadata chunk, and default (blank) XML content
metadata_chunkname = "Metadata"
metadata_chunktype = "Metadata"
metadata_default = inspect.cleandoc("""
    <?xml version="1.0"?>
    <info>
        <name>%s</name>
        <type>%s</type>
        <channel_count>0</channel_count>
        <nominal_srate>0</nominal_srate>
        <channel_format>string</channel_format>
        <source_id></source_id>
        <version>1.1000000000000001</version>
        <created_at>0</created_at>
        <uid>%s</uid>
        <session_id>default</session_id>
        <hostname>undefined</hostname>
        <desc></desc>
    </info>
    """ % (metadata_chunkname, metadata_chunktype, uuid.uuid4()))


# XDF chunk tags that we care about
class ChunkTags:
    STREAM_HEADER_TAG = 2
    SAMPLES_TAG = 3
    STREAM_FOOTER_TAG = 6


def read_varlen_int(fp):
    """Read a variable-length integer from a file handle."""
    nbytes = struct.unpack('B', fp.read(1))[0]
    if nbytes == 1:
        return struct.unpack('B', fp.read(1))[0]
    elif nbytes == 4:
        return struct.unpack('<I', fp.read(4))[0]
    elif nbytes == 8:
        return struct.unpack('<Q', fp.read(8))[0]
    else:
        raise RuntimeError('invalid variable-length integer encountered.')


# noinspection PyMethodMayBeStatic
def write_varlen_int(i, fp):
    """Write a variable-length integer into the given file-like object."""
    if i <= 0xFF:
        fp.write(struct.pack('<BB', 1, i))
    elif i <= 0xFFFFFFFFF:
        fp.write(struct.pack('<BL', 4, i))
    else:
        fp.write(struct.pack('<BQ', 8, i))


def write_chunk(fp, tag, content):
    """Write a chunk into an XDF file handle.

    Args:
        fp: file handle to use
        tag: chunk tag (see ChunkTags)
        content: byte content of the chunk
    """
    # write [NumLengthBytes] and [Length]
    write_varlen_int(len(content)+2, fp=fp)
    # write [Tag]
    fp.write(struct.pack('<H', tag))
    # write [Content]
    fp.write(content)


def scan_forward(f):
    """Scan forward through the given file object until after the next
    boundary chunk. This can be used for seeking or to skip corruptions."""
    blocklen = 2**20
    signature = bytes([0x43, 0xA5, 0x46, 0xDC, 0xCB, 0xF5, 0x41, 0x0F,
                       0xB3, 0x0E, 0xD5, 0x46, 0x73, 0x83, 0xCB, 0xE4])
    while True:
        curpos = f.tell()
        block = f.read(blocklen)
        matchpos = block.find(signature)
        if matchpos != -1:
            f.seek(curpos + matchpos + 15)
            logger.debug('  scan forward found a boundary chunk.')
            break
        if len(block) < blocklen:
            logger.debug('  scan forward reached end of file with no match.')
            break


def xml2dict(t):
    """Convert an attribute-less etree.Element into a dict."""
    dd = collections.defaultdict(list)
    for dc in map(xml2dict, list(t)):
        for k, v in dc.items():
            dd[k].append(v)
    return {t.tag: dd or t.text}


def matching_pathnames(paths):
    """Get list of matching pathnames for the given list of glob patterns."""
    results = []
    for p in paths:
        results.extend(glob.glob(p, recursive=True))
    return results


def gen_outpath(inpath, suffix, inplace):
    """Derive the output path for a given input path. Also returns whether
    the path is a temp pathname that shall be renamed to the original input
    path at the end.

    Args:
        inpath: path to input file
        suffix: the suffix to append to create the output path (e.g., 'processed')
        inplace: whether the input file is being processed in-place

    Returns:
        outpath: new path
        is_tempppath: whether the output path is a temp file that shall later be
          deleted

    """
    if inplace or not suffix:
        outpath = inpath + '.%i.tmp' % random.randint(10000, 99999)
        is_temppath = True
    else:
        outpath = inpath.replace('.xdf', suffix + '.xdf')
        is_temppath = False
    return outpath, is_temppath


def get_metadata_content(fh, filepath):
    """Extract the content of the metadata chunk and its span within the file.
    If no such chunk is present, default content will be returned, together with
    an empty span at the beginning of the stream headers section.

    Args:
        fh: file handle
        filepath: path of the file

    Returns:
        content: string content of the metadata chunk
        begin: begin position of the metadata chunk
        len: length in bytes of the metadata chunk in the file
        streamid: stream id of the metadata chunk
    """

    # number of bytes in the file (for fault tolerance)
    filesize = os.path.getsize(filepath)

    oldpos = fh.tell()

    # make sure we're reading from the beginnings
    fh.seek(0)

    # read [MagicCode]
    if fh.read(4) != b'XDF:':
        raise Exception('not a valid XDF file: %s' % filepath)

    # begin position of the stream headers in the ile
    streamheaders_begin = None
    # begin position & length of the metadata chunk, if encountered yet
    metadata_begin = None
    metadata_len = None
    # string content of the metadata chunk
    metadata_content = None
    # stream id of the existing metadata chunk, if any
    metadata_id = None
    # stream ids of other streams
    other_ids = []
    # whether we encountered more than one metadata chunk
    has_more_than_one = False

    # for each chunk...
    while True:
        begin_pos = fh.tell()

        # noinspection PyBroadException
        try:
            # read [NumLengthBytes], [Length]
            chunklen = read_varlen_int(fh)
        except Exception:
            if fh.tell() < filesize - 1024:
                logger.warning('got zero-length chunk, scanning forward to '
                               'next boundary chunk.')
                scan_forward(fh)
                continue
            else:
                logger.debug('  reached end of file.')
                break

        # read [Tag]
        tag = struct.unpack('<H', fh.read(2))[0]
        logger.debug('  read tag: %i at %d bytes, length=%d'
                     % (tag, fh.tell(), chunklen))

        # read the chunk's [Content]...
        if tag == ChunkTags.STREAM_HEADER_TAG:
            # read [StreamHeader] chunk...
            # note the beginning of the stream headers in the file
            if streamheaders_begin is None:
                streamheaders_begin = begin_pos
            # read [StreamId]
            stream_id = struct.unpack('<I', fh.read(4))[0]
            # read [Content]
            xml_string = fh.read(chunklen - 6)
            decoded_string = xml_string.decode('utf-8', 'replace')
            hdr = xml2dict(et.fromstring(decoded_string))
            if (hdr['info']['name'][0] == metadata_chunkname) and (hdr['info']['type'][0] == metadata_chunktype):
                if metadata_begin is None:
                    # found the first metadata chunk
                    metadata_begin = begin_pos
                    metadata_len = fh.tell() - begin_pos
                    metadata_content = decoded_string
                    metadata_id = stream_id
                else:
                    # found a subsequent one, ignore
                    has_more_than_one = True
                    other_ids.append(stream_id)
            else:
                other_ids.append(stream_id)
            # initialize per-stream temp data
            logger.debug('  found stream ' + hdr['info']['name'][0])
        elif tag in [ChunkTags.SAMPLES_TAG, ChunkTags.STREAM_FOOTER_TAG]:
            # got a [Samples] or [StreamFooter] chunk, so we're done traversing
            # headers at this point
            # (note: we ignore Metadata chunks that are not in the header
            # section of the file)
            if metadata_content is None:
                # if at this point we haven't encountered a metadata chunk,
                # we create one and note its desired insertion position
                metadata_content = metadata_default
                metadata_begin = streamheaders_begin
                metadata_len = 0
                # allocate a fresh stream id
                while True:
                    # we're using a high number here since we didn't scan past
                    # the end of the headers section, and it is possble that the
                    # file contains additional later headers; if so, these would
                    # have typically low IDs, and we'd clash with that otherwise
                    k = random.randint(10000, 99999)
                    if k not in other_ids:
                        metadata_id = k
                        break
            break
        else:
            # skip other chunk types (Boundary, ...)
            fh.read(chunklen - 2)

    if has_more_than_one:
        logging.warning("File %s has more than one metadata stream. "
                        "Using only the first one." % filepath)

    # restore old file cursor
    fh.seek(oldpos)

    return metadata_content, metadata_begin, metadata_len, metadata_id


def process_metadata_content(content, *, to_set, to_clear, to_show):
    """Process the given XML content string of the metadata chunk.

    Args:
        content: XML string
        to_set: list of "set" command-line directives (values to set, currently
          the only accepted form is 'name=value')
        to_clear: list of "clear" command-line directives (values to clear, just
          given as a list of field names)
        to_show: list of "show" directives (values to display, just a list of
          field names)

    Notes:
        field names can be given in the form "subject.age", which resolves to
        a path in the XML of the form desc/subject/age -- i.e., all custom
        fields are implicitly under "desc" (as per XDF standard)

    Returns:
        the updated content string
    """
    root = et.fromstring(content)
    desc = root.find('desc')
    for name in (to_show or []):
        # convert dot-name syntax into XPath slash syntax
        path = name.replace('.', '/')
        # find all nodes that match this name
        nodes = desc.findall(path)
        # print that
        for n in nodes:
            print('%s: %s' % (name, n.text))
    for name in (to_clear or []):
        # convert dot-name syntax into XPath slash syntax
        name = name.replace('.', '/')
        # find all nodes that match this name
        nodes = desc.findall(name)
        # find the parent of these nodes
        parent = desc.find(name + '/..')
        # now remove them from their respective parent node
        for n in nodes:
            parent.remove(n)
    for assignment in (to_set or []):
        name, value = assignment.split('=')
        name = name.replace('.', '/')
        node = desc.find(name)
        if node is not None:
            # node already exists, set value
            node.text = value
        else:
            # node doesn't exist yet, create path
            cur_node = desc
            for part in name.split('/'):
                next_node = cur_node.find(part)
                if next_node is None:
                    next_node = et.Element(part)
                    cur_node.append(next_node)
                cur_node = next_node
            # set value
            cur_node.text = value
    new_content = et.tostring(root).decode('utf-8')
    return new_content


def copy_range(inf, outf, length, blocksize=65536):
    """Copy a range from one file handle to another at the current respective
    positions.

    Args:
        inf: input file handle to read from
        outf: output file handle to write to
        blocksize: block size to use for copying, in bytes
    """
    while length >= blocksize:
        outf.write(inf.read(blocksize))
        length -= blocksize
    if length:
        outf.write(inf.read(length))


def process_file(inpath, outpath, *, to_set, to_clear, to_show, overwrite=False):
    """Process the given file, optionally writing the result into outpath.

    Args:
        inpath: input path
        outpath: output path
        to_set: list of "set" directives from the command-line
        to_clear: list of "clear" directives
        to_show: list of "show" directives
        overwrite: whether the --overwrite option was given, allowing existing
          files to be overwritten

    """
    logger.info("Processing file %s..." % inpath)

    # number of bytes in the file
    in_size = os.path.getsize(inpath)

    outf = None
    try:
        with open(inpath, 'rb') as inf:
            assert outpath != inpath, """Program error: outpath should not equal inpath."""
            if outpath:
                if not overwrite and os.path.exists(outpath):
                    raise FileExistsError("Output file already exists: %s. Use the --overwrite option to force-overwrite existing files." % outpath)
                outf = open(outpath, 'wb')

            # first read in the metadata chunk and note its place in the file
            meta_content, meta_begin, meta_len, meta_id = get_metadata_content(inf, inpath)

            # process the content and return the result of that
            new_content = process_metadata_content(meta_content,
                                                   to_set=to_set, to_clear=to_clear, to_show=to_show)

            if new_content != meta_content:
                # file has changed, need to splice chunk into file

                # first copy the part preceding the meta chunk
                copy_range(inf, outf, meta_begin)

                # write the new metadata chunk
                # write [StreamHeader] chunk
                # allocate fresh stream ID as bytes object for quick reuse
                chunkbytes = struct.pack('<L', meta_id) + new_content.encode('utf-8')
                write_chunk(outf, tag=ChunkTags.STREAM_HEADER_TAG, content=chunkbytes)

                # skip original version of header in input file
                inf.seek(meta_begin + meta_len)

                # finally copy the remainder of the file
                copy_range(inf, outf, in_size - meta_begin - meta_len)
            else:
                # no data change, just copy the file
                outf.close()
                shutil.copyfile(inpath, outpath)

    finally:
        if outf and not outf.closed:
            outf.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=inspect.cleandoc("""
        Manage XDF Tags. 
        Tags will be written into a stream named Metadata, of type Metadata, 
        and the stream will be created if not already present.
        
        You can use arguments like --set and --clear multiple times to set/clear
        multiple tags in a single run of the tool.
        
        Example:
        xdf-tagger --set subject.name="My Name" --set subject.id=subj001 --clear subject.handedness --show subject.age *.xdf
        
        """))
    parser.add_argument(
        '--set', action='append', help='Set or override the given name=value tag.'
    )
    parser.add_argument(
        '--clear', action='append', help='Clear the tag of the given name.'
    )
    parser.add_argument(
        '--show', action='append', help='Show the value for the given tag.'
    )
    parser.add_argument(
        '--suffix', default='.processed',
        help='Suffix that will be spliced in before the .xdf file ending. '
             'Ignored if --inplace is given.'
    )
    parser.add_argument(
        '--inplace', action='store_true',
        help='Process files in-place (temp files may still be generated).'
    )
    parser.add_argument(
        '--process-suffixed', action='store_true',
        help='Process files that already have the given suffix.'
    )
    parser.add_argument(
        '--overwrite', action='store_true',
        help='Allow overwriting existing files. Note that --inplace will always overwrite.'
    )
    parser.add_argument('--loglevel', default='INFO',
                        choices=['ERROR', 'WARN', 'INFO', 'DEBUG',
                                 'SUPERVERBOSE', 'NOTSET'],
                        help='Select logging level.')
    parser.add_argument(
        'paths', nargs='+', help='file paths (wildcard patterns) to process.'
    )
    a = parser.parse_args()

    logging.basicConfig(level=a.loglevel)

    is_modifying = a.set or a.clear

    # find all matching files
    inpaths = matching_pathnames(a.paths)

    # process each file
    for inpath in inpaths:
        # skip files that have our suffix
        if not a.process_suffixed and a.suffix:
            if inpath.endswith(a.suffix + '.xdf'):
                continue
        # determine output path
        if is_modifying:
            outpath, is_temppath = gen_outpath(inpath, suffix=a.suffix, inplace=a.inplace)
        else:
            outpath, is_temppath = None, False

        process_file(inpath, outpath,
                     to_set=a.set, to_clear=a.clear, to_show=a.show,
                     overwrite=a.overwrite)

        if is_temppath and outpath:
            # TODO: add support for keeping the file time
            os.remove(inpath)
            os.rename(outpath, inpath)
