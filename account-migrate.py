#!/usr/bin/python2.7
#coding=utf-8
#
# Copyright 2017 wuchangping
# Author: wuchangping
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

import argparse
import os, sys, json, time
from subprocess import Popen, PIPE

class SubProcError(Exception): pass


def user_info(tenant):
    try:
       # check user info
        cmd = 'radosgw-admin user info --uid=%s' % (tenant)
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise SubProcError()
        tags = json.loads(out)
        return tags, p.returncode
    except SubProcError:
        return None, p.returncode

def user_create(tenant):
    try:
       # create user 
        cmd = 'radosgw-admin user create --uid=%s --display-name="%s tenant"' % (tenant, tenant)
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise SubProcError()
        return p.returncode
    except SubProcError:
        print ("ERROR:  create tenant %s return code %s" % (tenant, p.returncode))
        return p.returncode

		
def subuser_create(tenant, username, key):
    try:
       # create subuser
        cmd = 'radosgw-admin subuser create --uid=%s --subuser=%s:%s --access=full' % (tenant, tenant, username)
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise SubProcError()
        return p.returncode
    except SubProcError:
        print ("ERROR:  create subuser %s return code %s" % (username, p.returncode))
        return p.returncode

def key_create(tenant, username, key):
    try:
       # create key
        cmd = 'radosgw-admin key create --subuser=%s:%s --key-type=swift --secret-key=%s' % (tenant, username, key)
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise SubProcError()
        return p.returncode
    except SubProcError:
        print ("ERROR:  create key %s return code %s" % (key, p.returncode))
        return p.returncode

		
		
def radosgw_admin(tenant, username, key):
    uname = tenant + ':' + username
    tags, err = user_info(tenant)
    if err != 0:
        ret = user_create(tenant)
        if ret == 0:
            ret = subuser_create(tenant, username,key)
            #print ("subuser create ret %s" % ret)
            if ret == 0:
                ret = key_create(tenant, username, key)
                #print ("key create ret %s" % ret)
                if ret != 0:
                    sys.exit(1)
            else:
                sys.exit(1)
            
        else:
            sys.exit(1)
		
    else:
        if len(tags['swift_keys']) > 0:
            for i in range(len(tags['swift_keys'])):
                #print ("%s, %s" % (tags['swift_keys'][i]['user'], uname))
                if tags['swift_keys'][i]['user'] == uname:
                    user_exist = True
                    break
                else:
                    user_exist = False
            if user_exist == False:
                ret = subuser_create(tenant, username, key)
                #print ("new subuser create ret %s" % ret)
                if ret == 0:
                    ret = key_create(tenant, username, key)
                    #print ("new key ccreate ret %s" % ret)
                    if ret != 0:
                        sys.exit(1)
                else:
                    sys.exit(1)
    return 0

						
def get_account():
    f = open('user.txt', 'r')
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
    tenants, user, key ,cnt = get_account()
    for i in range(cnt):
        ret = radosgw_admin(tenants[i], user[i], key[i])
        if ret != 0:
            print ("ERROR: radosgw admin error!!!")
        time.sleep(1)   # wait 
  
  
if __name__ == '__main__':
    main()

	
	
	
	
	

	
	
	
	
	
	
	
	
	
	
	
