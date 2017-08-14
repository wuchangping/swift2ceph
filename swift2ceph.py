#!/usr/bin/python2.7
#
# Copyright 2016 Catalyst IT Ltd
# Author: lingxian.kong@catalyst.net.nz
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# Copyright 2017 wuchangping
# Author: wuchangping
#

import argparse
from collections import Iterable
import getpass
import json
import multiprocessing
import os
import re
import sys
import tempfile
import time
import traceback

import six
import swiftclient
from swiftclient.service import SwiftError
from swiftclient.service import SwiftUploadObject

import util

reload(sys)
sys.setdefaultencoding('utf-8')


# need b6457e0f95c2563f745bbfb64c739929bc0dc901 for body
# response object in get_object that supports read() method
if swiftclient.__version__ < '2.6.0':
    raise RuntimeError('newer swiftclient required')

# The custom header we specify when creating object in Swift, to help us
# indentify original large object uploaded with S3 API in RGW.
OLD_HASH_HEADER = 'x-object-meta-old-hash'

# The custom header we specify when creating object in Swift, to help us
# indentify original timestamp of DLO in RGW.
OLD_TIMESTAMP_HEADER = 'x-object-meta-old-timestamp'

# A simple regex that matches large object hash which uploaded with S3
# multi-part upload API.
HASH_PATTERN = re.compile('\w+-\w+')

GB_5 = 5368709120
GB_SPLIT = 2147483648

def print_warning():
    print 70 * '='
    print "WARNING:Before copy step, the accounts must be migrate from swift"
    print "       :to ceph at ceph RGW gateway node with account-migrate.py"
    print 70 * '='

def _print_object_detail(src_srvclient, tenant_name, cname, content,
                         max_size_info, object=None):
    if object:
        stat_res = list(
            src_srvclient.stat(container=cname, objects=[object])
        )[0]

        if not stat_res['success']:
            raise Exception(stat_res["error"])

        header = stat_res['headers']
        item = stat_res['items']
        if int(item[4][1]) > int(max_size_info['size']):
            max_size_info.update({
                'tenant': tenant_name,
                'size': int(item[4][1]),
                'container': cname,
                'object': object
            })

        prefix = ('[large-object] '
                  if HASH_PATTERN.match(header['etag'])
                  else '')
        content.append(
            '            %s%s\t%s' % (
                prefix,
                object,
                int(item[4][1]),
            )
        )
        content.append('            ....headers: %s' % header)

        return

    for page in src_srvclient.list(container=cname):
        if page["success"]:
            object_names = [o['name'] for o in page["listing"]]
            objects = list(
                src_srvclient.stat(
                    container=cname,
                    objects=object_names)
            )
            object_mapping = {}
            for o in objects:
                object_mapping[o['object']] = o

            for item in page["listing"]:
                if item['bytes'] > max_size_info['size']:
                    max_size_info.update({
                        'tenant': tenant_name,
                        'size': item['bytes'],
                        'container': cname,
                        'object': item['name']
                    })

                prefix = ('[large-object] '
                          if HASH_PATTERN.match(item['hash'])
                          else '')

                obj_stat = object_mapping[item['name']]
                obj_header = obj_stat['headers']

                content.append(
                    '            %s%s\t%s' % (
                        prefix,
                        item['name'],
                        item['bytes'],
                    )
                )
                content.append('            ....headers: %s' % obj_header)
        else:
            raise Exception(page["error"])


def print_info(elapsed, stats, tenant_usage, moved_stats):
    # Print total object storage information.
    print 70 * '='
    print "Elapsed time: {0}s".format(elapsed)
    print(
        "Total containers: %s, objects: %s, size: %.3fT" % (
            stats['cons'],
            stats['objs'],
            float(stats['bytes']) / (1024 * 1024 * 1024 * 1024)
        )
    )

    # Print tenant usage in descending order.
    sorted_tenant_usage = sorted(
        tenant_usage.items(), key=lambda d: d[1], reverse=True)
    print('Tenants have objects:')
    for name, usage in sorted_tenant_usage:
        if usage > 0:
            print('%35s: %.6fG' % (name, float(usage) / (1024 * 1024 * 1024)))

    # Print total moved objects.
    print 70 * '='
    total_moved_count = 0
    total_moved_bytes = 0
   # for tenant, moved in six.iteritems(moved_stats):
    for key in list(moved_stats.keys()):
        moved = moved_stats[key]
        total_moved_count += moved['moved_objects']
        total_moved_bytes += moved['moved_bytes']


    print "Total moved objects:"
    print(
        "\tobjects: %s, size: %.3fG" % (
            total_moved_count,
            float(total_moved_bytes) / (1024 * 1024 * 1024)
        )
    )

    # Print moved objects per tenant.
    print('Moved objects per tenant:')
    #for tenant, moved in six.iteritems(moved_stats):
    for key in list(moved_stats.keys()):
       moved = moved_stats[key]
       if moved['moved_bytes'] > 0:
            print(
                '\t%s: objects: %s, size: %s' %
                (key, moved['moved_objects'], moved['moved_bytes'])
            )


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u", "--user",
        help="Combination of admin tenant name and user name. Example: "
             "openstack:objectmonitor"
    )
    parser.add_argument(
        "-k", "--key",
        help="secret key"
    )
    parser.add_argument("-s", "--srcauthurl", help="swift auth url")
    parser.add_argument("-t", "--tgtauthurl", help="ceph rgw auth url")
    parser.add_argument(
        "-r", "--srcregion",
        help="swift region in which the migration needs to happen"
    )
    parser.add_argument(
        "-g", "--tgtregion",
        help="ceph rgw region in which the migration needs to happen"
    )
    parser.add_argument(
        "-a", "--act",
        choices=['stat', 'copy'],
        default="stat",
        help="Action to be performed. 'stat' means only get statistic of "
             "object storage without migration, 'copy' means doing migration. "
             "Default: stat"
    )
    parser.add_argument(
        "-v", "--verbose",
        action='store_true',
        help="verbose",
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        help="Number of processes need to be running. Default: 1",
        default=1
    )
    parser.add_argument(
        "--container",
        help="Container name needs to migrate.",
    )
    parser.add_argument(
        "--object",
        help="Object name needs to migrate.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-i', '--include-tenants', nargs='*',
        help="List of tenants to include in migration, delimited "
             "by whitespace."
    )
    group.add_argument(
        '-e', '--exclude-tenants', nargs='*',
        help="List of tenants to exclude in migration, delimited "
             "by whitespace."
    )
    group.add_argument(
        '--include-file',
        help="A file that contains a list of project names that should be "
             "included.",
        default="user.txt"
    )
    group.add_argument(
        '--exclude-file',
        help="A file that contains a list of project names that should be "
             "excluded."
    )

    return parser


def stat_tenant(id, content, src_srvclient, max_size_info, tenant_name,
                container=None, object=None):
    if container:
        print('...[%02d] Processing container %s' % (id, container))

        stat_res = src_srvclient.stat(container=container)
        if stat_res['success']:
            header = stat_res['headers']
            content.append(
                '........{0}, objects: {1}, bytes: {2}'.format(
                    container, header['x-container-object-count'],
                    header['x-container-bytes-used'])
            )

            if object:
                _print_object_detail(src_srvclient, tenant_name, container,
                                     content, max_size_info, object=object)

            return
        else:
            raise Exception(stat_res["error"])

    for page in src_srvclient.list():
        if page["success"]:
            for container in page["listing"]:
                cname = container['name']
                print('...[%02d] Processing container %s' % (id, cname))

                content.append(
                    '........{0}, objects: {1}\tbytes: {2}'.format(
                        cname, container['count'], container['bytes'])
                )

                # Print objects details.
                _print_object_detail(src_srvclient, tenant_name, cname,
                                     content, max_size_info)
        else:
            raise Exception(page["error"])


def check_migrate_object(container_name, src_header, tgt_obj):
    """Check whether we should migrate src object or not.

    Return True if migration is needed, otherwise return False.
    """
    if not tgt_obj['success']:
        return True

    tgt_header = tgt_obj['headers']
    src_etag = src_header['etag']
    origin_timestamp = float(src_header['x-timestamp'])
    tgt_timestamp = float(tgt_header['x-timestamp'])

    # For multi-part large object hash check.
    if (HASH_PATTERN.match(src_etag) and
            tgt_header.get(OLD_HASH_HEADER, '') == src_etag):
        return False

    # For DLO, skip the migration if the timestamp has not changed.
    if src_header.get('x-object-manifest', False):
        origin_length = int(src_header['content-length'])
        tgt_length = int(tgt_header['content-length'])
        tgt_timestamp = tgt_header.get(OLD_TIMESTAMP_HEADER, '0')

        # Do not move object if they have the same length and latest version
        # on Swift side.
        if origin_length == tgt_length and origin_timestamp <= tgt_timestamp:
            return False

        return True

    # For normal object etag check. For some reason, the hash in
    # 'container_list' output has '\x00' in the end.
    if tgt_header['etag'] == src_etag.replace('\x00', ''):
        return False
    # Do not move object if the lasted version is on Swift side
    elif origin_timestamp <= tgt_timestamp:
        return False

    return True


def check_migrate_after(tgt_container_name, object_name, src_etag, tgt_srvclient,
                        is_dlo, content):
    content.append("             ..ok..checking")

    time.sleep(1) #wait
	
    if object_name[0] == '/':
        object_name = object_name.lstrip('/')
	
    tgt_obj = list(
        tgt_srvclient.stat(
            container=tgt_container_name,
            objects=[object_name])
    )[0]

    if not tgt_obj['success']:
        raise Exception(tgt_obj['error'])

    if not is_dlo:
        # For some reason, the hash in 'container_list' output has '\x00' in
        # the end.
        tgt_header = tgt_obj['headers']
        if (not tgt_header.get(OLD_HASH_HEADER, False) and
                tgt_header['etag'] != src_etag.replace('\x00', '')):
            raise Exception('src and target objects have different hashes.')

    content.append("             ..ok")


def migrate_DLO(src_container_name,tgt_container_name, object_name, src_head, src_srvclient,
                tgt_srvclient):
    """Migrate dynamic large object."""
    headers = ['x-object-manifest:%s' % src_head['x-object-manifest']]
    headers.append(
        '%s:%s' % (OLD_TIMESTAMP_HEADER, src_head['x-timestamp']))

    upload_iter = tgt_srvclient.upload(
        tgt_container_name,
        [SwiftUploadObject(None, object_name=object_name)],
        options={'header': headers}
    )

    for r in upload_iter:
        if not r['success']:
            raise Exception(r['error'])


def migrate_SLO(src_container_name,tgt_container_name, object_name, src_head, src_srvclient,
                tgt_srvclient):
    """Migrate static large object.

    Note that static large object (slo) does not actually work with RGW as of
    0.9.4.x, so hopefully not too many of these. We migrate them anyway.

    It's a little difficult to upload the SLO manifest file before we are sure
    all the segments are uploaded to Swift. When the PUT operation sees the
    ?multipart-manifest=put query parameter, it reads the request body and
    verifies that each segment object exists and that the sizes and ETags
    match. If there is a mismatch, the PUT operation fails.
    """
    with tempfile.NamedTemporaryFile() as temp_file:
        down_res_iter = src_srvclient.download(
            container=src_container_name,
            objects=[object_name],
            options={'out_file': temp_file.name, 'checksum': False}
        )

        for down_res in down_res_iter:
            if down_res['success']:
                headers = ['x-static-large-object:True']

                upload_iter = tgt_srvclient.upload(
                    tgt_container_name,
                    [SwiftUploadObject(temp_file.name,
                                       object_name=object_name)],
                    options={'header': headers}
                )
                for r in upload_iter:
                    if not r['success']:
                        raise Exception(r['error'])
            else:
                raise Exception(down_res['error'])

    # src_objtype = src_head['content-type']
    # src_objlen = src_head['content-length']
    #
    # tgt_ohead = {}
    # tgt_ohead['x-static-large-object'] = src_head['x-static-large-object']
    #
    # src_obj = src_swiftcon.get_object(
    #     container_name, src_object['name'],
    #     query_string='multipart-manifest=get'
    # )
    # src_objdata = src_obj[1]
    #
    # # reconstruct valid manifest from saved debug manifest object
    # # body - the dict keys are *wrong*...wtf
    # tgt_parsed_objdata = []
    # src_parsed_objdata = json.loads(src_objdata)
    #
    # for parsed_item in src_parsed_objdata:
    #     parsed_item['size_bytes'] = parsed_item.pop('bytes')
    #     parsed_item['etag'] = parsed_item.pop('hash')
    #     parsed_item['path'] = parsed_item.pop('name')
    #     for key in parsed_item.keys():
    #         if (key != 'size_bytes' and key != 'etag'
    #             and key != 'path' and key != 'range'):
    #             parsed_item.pop(key)
    #     tgt_parsed_objdata.append(parsed_item)
    #
    # tgt_objdata = json.dumps(tgt_parsed_objdata)
    #
    # tgt_swiftcon.put_object(container_name, src_object['name'],
    #                         contents=tgt_objdata, content_type=src_objtype,
    #                         content_length=src_objlen, headers=tgt_ohead)


class _ReadableContent(object):
    def __init__(self, reader):
        self.reader = reader
        self.content_iterator = iter(self.reader)

    def read(self, chunk_size):
        """Read content of object.

        the 'chunk_size' param is not used here, but it's passed by uploading
        process. For object downloading and uploading, the default
        chunk size is the same(65536 by default).
        """
        return self.content_iterator.next()


def get_object_user_meta(object_header):
    user_meta_list = []

    #for (m_key, m_value) in six.iteritems(object_header):
    for m_key in list(object_header.keys()):
        if m_key.lower().startswith('x-object-meta-'):
            m_value = object_header[m_key]     
            user_meta_list.append('%s:%s' % (m_key, m_value))

    return user_meta_list


def migrate_object(src_container_name,tgt_container_name, object_name, src_byte, src_head,
                   src_srvclient, tgt_srvclient, content):
    """Migrate normal object."""
    single_large_object = True if int(src_byte) > GB_5 else False

    # Get user's customized object metadata, format:
    # X-<type>-Meta-<key>: <value>
    # http://docs.openstack.org/developer/swift/development_middleware.html#swift-metadata
    user_meta = get_object_user_meta(src_head)

    header_list = []
    if HASH_PATTERN.match(src_head['etag']):
        header_list.append('%s:%s' % (OLD_HASH_HEADER, src_head['etag']))
    header_list.extend(user_meta)

    if single_large_object:
        content.append('            ..[large object]download...split...upload')

        with tempfile.NamedTemporaryFile() as temp_file:
            down_res = list(src_srvclient.download(
                containaer=src_container_name,
                objects=[object_name],
                options={'out_file': '-', 'checksum': False}
            ))[0]
            contents = down_res['contents']
            assert isinstance(contents, Iterable)

            # Hacking here to avoid download checksum.
            # TODO: Remove this when swiftclient version > 3.0.0
            contents._expected_etag = None

            for chunk in contents:
                temp_file.file.write(chunk)

            # Upload large object with segments.
            upload_iter = tgt_srvclient.upload(
                tgt_container_name,
                [SwiftUploadObject(temp_file.name,
                                   object_name=object_name)],
                options={'header': header_list,
                         'segment_size': GB_SPLIT,
                         'checksum': False}
            )
            for r in upload_iter:
                if not r['success']:
                    raise Exception(r['error'])
    else:
        # Download normal object, the contents in response is an iterable
        # object. We don't do checksum for download/upload, will do that
        # outside this method.
        down_res = list(
            src_srvclient.download(
                container=src_container_name,
                objects=[object_name],
                options={'out_file': '-', 'checksum': False}
            )
        )[0]
        contents = down_res['contents']
        assert isinstance(contents, Iterable)

        # Hacking here to avoid download checksum.
        # TODO: Remove this when swiftclient version > 3.0.0
        contents._expected_etag = None

        readalbe_content = _ReadableContent(contents)

        upload_iter = tgt_srvclient.upload(
            tgt_container_name,
            [SwiftUploadObject(readalbe_content,
                               object_name=object_name)],
            options={'header': header_list, 'checksum': False}
        )

        for r in upload_iter:
            if not r['success']:
                raise Exception(r['error'])


def migrate_container(src_container_name,tgt_container_name, src_srvclient, tgt_srvclient, content,
                      object=None, moved_stats=None):
    if object:
        list_res = [
            {
                'success': True,
                'listing': [{'name': object}]
            }
        ]
    else:
        list_res = src_srvclient.list(container=src_container_name)

    for page in list_res:
        if not page["success"]:
            raise Exception(page["error"])

        # Get all the objects status by bulk query to save API calls.
        object_names = [o['name'] for o in page["listing"]]
        objects = list(
            src_srvclient.stat(
                container=src_container_name,
                objects=object_names)
        )
        object_mapping = {}
        for o in objects:
            object_mapping[o['object']] = o

        # Do the same for target object storage.
        tgt_objects = list(
            tgt_srvclient.stat(
                container=tgt_container_name,
                objects=object_names)
        )
        tgt_object_mapping = {}
        for o in tgt_objects:
            tgt_object_mapping[o['object']] = o

        for src_data in page["listing"]:
            object_name = src_data['name']
            src_obj = object_mapping[object_name]
            tgt_obj = tgt_object_mapping[object_name]
            src_ohead = src_obj['headers']
            src_byte = src_obj['items'][4][1]
            is_dlo = src_ohead.get('x-object-manifest', False)

            # First, check if migration is needed.
            if not check_migrate_object(src_container_name, src_ohead, tgt_obj):
                content.append(
                    '            existing object: %s' % object_name)
                continue

            content.append(
                '            creating object: %s,\tbytes: %s' %
                (object_name, src_byte))

            try:
                if is_dlo:
                    migrate_DLO(src_container_name,tgt_container_name, object_name, src_ohead,
                                src_srvclient, tgt_srvclient)
                elif src_ohead.get('x-static-large-object', False):
                    # This is not gonna happen.
                    migrate_SLO(src_container_name,tgt_container_name, object_name, src_ohead,
                                src_srvclient, tgt_srvclient)
                else:
                    migrate_object(src_container_name,tgt_container_name, object_name, src_byte,
                                   src_ohead, src_srvclient, tgt_srvclient,
                                   content)

                # Check hash and etag after uploading, don't check DLO.
                check_migrate_after(
                    tgt_container_name, object_name, src_ohead['etag'],
                    tgt_srvclient, is_dlo, content
                )

                # Update moved stats
                moved_stats['moved_objects'] += 1
                if not is_dlo:
                    moved_stats['moved_bytes'] += int(src_byte)
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value,
                                                   exc_traceback)
                err_msg = ''.join(line for line in lines)
                content.append("             ..failed. Reason: %s" % err_msg)


def migrate_tenant(id, content, src_srvclient, tgt_srvclient, container=None,
                   object=None, moved_stats=None):
    if container:
        list_res = [
            {
                'success': True,
                'listing': [{'name': container}]
            }
        ]
    else:
        list_res = src_srvclient.list()

    for page in list_res:
        if page["success"]:
            for container in page["listing"]:
                src_cname = container['name']
                print ('...[%02d] Processing container %s' % (id, src_cname))

                containe_stat = src_srvclient.stat(container=src_cname)
                header = containe_stat['headers']
                
                #print ('...Processing container containe_stat %s' % (containe_stat))
                #print ('...Processing container header %s' % (header))

                header_list = []
                if header.has_key('x-container-read'):
                    header_list.append(
                        "X-Container-Read:%s" % header['x-container-read'])
                if header.has_key('x-container-write'):
                    header_list.append(
                        "X-Container-Write:%s" % header['x-container-write'])
                tgt_options = {'header': header_list}

                # create container in target if it does not exist
                try:
                   if len(src_cname) < 3:
                       tgt_cname = 'docker-bucket-1-%s' % (src_cname)
                   else:
                       tgt_cname = src_cname

                   stat = tgt_srvclient.stat(container=tgt_cname)
                except SwiftError:
                    content.append('........creating container: %s' % tgt_cname)
                    try:
                        tgt_srvclient.post(container=tgt_cname,
                                           options=tgt_options)
                        content.append("........ok")
                    except SwiftError as e:
                        content.append("........failed. Reason: %s" % str(e))
                        continue
                else:
                    content.append('........existing container: %s' % tgt_cname)

                print ('...Processing container migrate content %s object %s' % (content,object))

                migrate_container(src_cname,tgt_cname, src_srvclient, tgt_srvclient, content,
                                  object=object, moved_stats=moved_stats)

        else:
            raise Exception(page["error"])


def _get_service_clients(tenant, user, key, args):
    tgt_srvclient = None
    
    src_srvclient = util.get_service_client_v2(
        tenant,
        user,
        key,
        args.srcauthurl,
        {'os_region_name': args.srcregion}
    )

    if args.act == 'copy':
        tgt_srvclient = util.get_service_client_v1(
            tenant,
            user,
            key,
            args.tgtauthurl,
            {'os_region_name': args.tgtregion}
        )
       
    return src_srvclient, tgt_srvclient


def worker(id, tenants, lock, stats, moved_stats, tenant_usage, args, key, user):
    file_name = ("worker-%02d.output" % id)
    max_size_info = {'tenant': '', 'container': '', 'object': '', 'size': 0}

    # Remove the log file first.
    if os.path.exists(file_name):
        os.remove(file_name)
    
    
  #  for tenant in tenants:
    tenant = tenants
    if True:
        content = []
        moved_stats[tenant] = {'moved_objects': 0, 'moved_bytes': 0}

        try:
            print('[%02d] processing tenant: %s, user: %s, key: %s' % (id, tenant, user, key))
            content.append("....processing tenant " + tenant)

            src_srvclient, tgt_srvclient = _get_service_clients(
                tenant, user, key, args)

            with src_srvclient:
                accout_stat = src_srvclient.stat()

               # print('account_stat %s' % (accout_stat))

                account = accout_stat['headers']
              #  print('account %s' % (account))

                content.append(
                    "......containers: {0}, objects: {1}, bytes: {2}".format(
                        account['x-account-container-count'],
                        account['x-account-object-count'],
                        account['x-account-bytes-used']
                    )
                )
                tenant_usage[tenant] = int(
                    account['x-account-bytes-used']
                )

                if lock:
                    with lock:
                        stats['cons'] += int(
                            account['x-account-container-count'])
                        stats['objs'] += int(account['x-account-object-count'])
                        stats['bytes'] += int(account['x-account-bytes-used'])
                else:
                    stats['cons'] += int(account['x-account-container-count'])
                    stats['objs'] += int(account['x-account-object-count'])
                    stats['bytes'] += int(account['x-account-bytes-used'])

                if int(account['x-account-container-count']) > 0:
                    if args.act == 'stat' and (args.verbose or args.object):
                        stat_tenant(id, content, src_srvclient, max_size_info,
                                    tenant.name, container=args.container,
                                    object=args.object)
                    if args.act == 'copy':
                        with tgt_srvclient:
                            migrate_tenant(
                                id, content, src_srvclient, tgt_srvclient,
                                container=args.container, object=args.object,
                                moved_stats=moved_stats[tenant]
                            )
        except Exception as e:
            print(
                '[%02d] error occured when processing tenant: %s. error: %s' %
                (id, tenant, str(e))
            )

            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=2, file=sys.stdout)
        finally:
            with open(file_name, 'a') as file:
                file.write('\n'.join(content))
                file.write('\n')

    # Print max object information.
    if args.act == 'stat' and args.verbose:
        with open(file_name, 'a') as file:
            file.write('\nmax object size info: %s' % max_size_info)

			
def get_account(args):
    tenant = list()
    user = list()
    key = list()
    if args.user and args.key:
        print ("user %s, key %s" % (args.user, args.key))
        tenant.append(args.user.split(':')[0])
        user.append(args.user.split(':')[1])
        key.append(args.key)
        return tenant, user, key, 1

    if os.path.exists(args.include_file) == False:
        print ("%s is not exist, return None" % (args.include_file))
        return None, None, None, 0

    f = open(args.include_file, 'r')
    lst = list()  
    for line in f.readlines():
        line = line.strip()
        if not len(line) or line.startswith('#'):
            continue
        lst.append(line) 	

    tenant = list()
    user = list()
    key = list()
    for i in range(len(lst)):
        tmp = lst[i].split()
        tenant.append(tmp[0])
        user.append(tmp[1])
        key.append(tmp[2])
		
    return tenant, user, key, (i+1)



def main():
    print_warning()

    parser = get_parser()
    args = parser.parse_args()

    tenants_group, user, key ,cnt = get_account(args)
    if tenants_group == None:
        sys.exit(1)

    if (args.container and (len(tenants_group) != 1)):
        print('Error: Only one tenant can be specifed when specifying '
              'container to migrate.')
        sys.exit(1)
    if args.object and not args.container:
        print('Error: Container must be specified together with object.')
        sys.exit(1)

    print ("\nStart migration in %s processes. The output of each process is "
          "contained in separated file under the script's directory.\n"
          % len(tenants_group))

    stats = {'cons': 0, 'objs': 0, 'bytes': 0}
    moved_stats = {}
    tenant_usage = {}
    elapsed = time.time()

    if len(tenants_group) > 1:
        jobs = []
        lock = multiprocessing.Lock()
        manager = multiprocessing.Manager()
        stats = manager.dict({'cons': 0, 'objs': 0, 'bytes': 0})
        moved_stats = manager.dict()
        tenant_usage = manager.dict()

        for i in range(len(tenants_group)):
            p = multiprocessing.Process(
                target=worker,
                args=(i, tenants_group[i], lock, stats, moved_stats,
                      tenant_usage, args, key[i], user[i])
            )
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
    else:
        worker(0, tenants_group[0], None, stats, moved_stats, tenant_usage, args, key[0], user[0])

    elapsed = time.time() - elapsed
    print_info(elapsed, stats, tenant_usage, moved_stats)


if __name__ == '__main__':
    main()
