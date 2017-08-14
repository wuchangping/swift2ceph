# swift2ceph
 Object Storage Migration From swift to Ceph RGW
============================================================================
* The tool base on https://github.com/LingxianKong/swift-migration-from-rgw
============================================================================

Migrating data from swift to ceph RGW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Prerequisites

    * It's recommended running this script on ceph RGW node in each region.
    * python-swiftclient and python-keystoneclient need to be installed.
    * Please ensure there is enough disk space(maximum of
      concurrency*max_single_object_size) in /tmp on the host the script is
      running, because the script will download large object to that folder,
      then delete it after upload.
	  
2. Before actual moving objects from swift to ceph RGW, must migrate the accounts:
	 
	$ ls
	account-migrate.py  swift2ceph.py  user.txt  util.py
	* add the account at user.txt , format : tenant subuser password, just like:
	$ cat user.txt 
	admin ksadmin ksadmin
	$./account-migrate.py

	
2. Before actual moving objects from swift to ceph RGW, you can see the overview of
   object storage statistics in swift:

    $./swift2ceph.py -s swift_authurl --act stat
	* example:	 
    $./swift2ceph.py  -s http://x.x.x.x:35357/v2.0 --act stat

3. Start to migrate object from swift to ceph RGW:

    $./swift2ceph.py -s swift authurl -t ceph authurl --act copy
	* example:	 
    $./swift2ceph.py  -s http://x.x.x.x.x:35357/v2.0 -t http://x.x.x.x.x:10080/auth/v1.0 --act copy

