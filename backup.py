#!/usr/bin/python
import datetime
import os
import sys
import shutil
import re
import traceback

do_debug = False
return_code= 0

class MyError(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)

def minor(msg):
	print datetime.datetime.now().strftime('[%Y-%m-%d %H:%M:%S]'), msg

def major (msg):
	minor("*** " + msg)

def debug(msg):
	if do_debug == True:
		minor(msg)

def warn(msg):
	minor("*W* " + msg)

def error(msg, code=0):
	global return_code
	minor("*!* " + msg)
	# Set error code
	if code > return_code:
		return_code = code

def logged_command(cmd):
	#minor(cmd)
	return os.system(cmd)

def do_backup(config, base, path, oldest):
	#now = "d-" + str(point) + ".03:03:03"
	now = datetime.datetime.now().strftime('d-%Y-%m-%d.%H:%M:%S')
	exc, policy = config['policy'][path]
	d,w,m = policy

	if d <= 0:
		warn("XXX won't back up as d is 0, this needs more work")
	

	# Excludes
	excludes = ''
	for i in exc:
		excludes += " --exclude '%s' " % i

	file_base = base + 'backup/' +config['VP']['HOST'] + '/' + path
	log_base = base + 'logs/' +config['VP']['HOST'] + '/' + path

	if 'IP' in config['VP'].keys():
		host = config['VP']['IP']
	else:
		host = config['VP']['HOST']

	link = ''
	if oldest is not None:
		link_base = file_base + '/' + oldest.strftime('d-%Y-%m-%d.%H:%M:%S')
		link = "--link-dest=" + link_base

	if not os.path.lexists(file_base):
		os.makedirs(file_base)

	if not os.path.lexists(log_base):
		os.makedirs(log_base)

	minor("Backing up %s to %s" % (path + '/', file_base + '/' + now))
	ret = logged_command("rsync \
		-v \
		-a \
		--numeric-ids \
		--blocking-io \
		--partial \
		--verbose \
		--delete \
		%s \
		%s \
		-e 'ssh -i %s -o BatchMode=yes %s' \
		%s \
		'%s@%s:%s' '%s' > %s" % (excludes,
						link,
						config['VP']['KEY'],
						config['VP']['SSH_OPTIONS'],
						config['VP']['RSYNC_OPTIONS'],
						config['VP']['USER'],
						host,
						path + '/',
						file_base + '/' + now,
						log_base + '/' + now)
		)

	# Check return code, ignoring 'File Vanished'
	if ret !=0 and ret !=24:
		error("rsync returned error code %s" % ret, ret)


	# Compress log file
	ret = logged_command("gzip %s" % log_base + '/' + now)
	if ret !=0:
		error("gzip returned error code %s" % ret, ret)
	


# This is the heart of the clean and rotate procedure,
# base is the root folder to work in
# d1 is the list of folders in the lvl we might be moving backups to
# d2 is the list of folders that lower lvl doesn't want anymore
# c1 and c2 is the char handle of the lvls for d1 and d2 respectivly
# What we do is look at what d1 needs, and see if d2 can meet those needs,
# if it can we take from d2 to d1 what we need, and return the new d1. Whaterver
# we don't used from d2, we delete.
def generic(base,d1,d2,delta,c1,c2):
	d2.sort()
	# While we have something to process and are not just deleting
	while len(d2) > 0 and c1 != 'u':
		c = len(d1)

		# if d1 is empty then obviously we need something from d2, gobble the
		# oldest entry in d2 because we might be able to get more in the next
		# iteration
		if c == 0:
			src = d2[0].strftime("%s-%%Y-%%m-%%d.%%H:%%M:%%S"%c2)
			dst = d2[0].strftime("%s-%%Y-%%m-%%d.%%H:%%M:%%S"%c1)
			debug("Lvl %s is empty, Move %s to %s" % (c1,src,dst))
			minor("Move %s/%s to %s/%s" % (base,src,base,dst))
			os.rename("%s/%s" % (base,src), "%s/%s" % (base,dst))
			d1 = [d2[0]] + d1
			d2 = d2[1:]
		else:
			# Ok d1 is not empty, compare the date of the newest entry in d1
			# to now and delta to see if we need to search for somthing in d2
			newest = d1[0]
			
			if d2[-1].date() - newest.date() >= delta:
				# We need search d2 from oldest to newest and find first entry
				# that is new enough to meet d1's criteria
				debug("Lvl %s is %s old" % (c1,d2[0].date() - newest.date()))
				target = newest + delta
				found = False;
				for d in d2:
					if d.date() >= target.date():
						# We found first entry that meets d1's criteria, gobble
						# it and end the inner loop
						src = d.strftime("%s-%%Y-%%m-%%d.%%H:%%M:%%S"%c2)
						dst = d.strftime("%s-%%Y-%%m-%%d.%%H:%%M:%%S"%c1)
						minor("Move %s/%s to %s/%s" % (base,src,base,dst))
						os.rename("%s/%s" % (base,src), "%s/%s" % (base,dst))
						d1 = [d] + d1
						d2.remove(d)
						found = True;
						break
				# If we went through all of d2 and found nothing for d1, stop
				# the outer loop, we won't find something next time either
				if not found:
					break
			else:
				# D1 doesn't need anything, we are done
				break

	# d1 now has taken anything it can from d2, anything in d2 now needs to be
	# removed from the filesystem
	for d in d2:
		src = d.strftime("%s-%%Y-%%m-%%d.%%H:%%M:%%S"%c2)
		minor("Remove %s/%s" %(base,src))
		shutil.rmtree("%s/%s" % (base,src))
	return d1


# Helpder function to migrate daily backups to the weekly level
# that uses gerneric for the work
def weekly_from_daily(base, weekly,daily):
	return generic(base, weekly, daily, datetime.timedelta(days=7), 'w', 'd')

# Helpder function to migrate weekly backups to the monthly level
# that uses gerneric for the work
def monthly_from_weekly(base, monthly,weekly):
	return generic(base, monthly, weekly, datetime.timedelta(days=30), 'm', 'w')

# Helpder function to prune things from the monthly level that are now old
# that uses gerneric for the work
def remove_from_monthly(base, monthly):
	return generic(base, [], monthly, datetime.timedelta(days=30), 'u', 'm')

# This function will search for backups to rotate into older achrives
# or delete from a directory
def clean_and_rotate(base, profile):
	days, weeks, months = profile
	monthly = []
	weekly = []
	daily = []

	# Iterate through a directory finding folders that look like backups
	# and loading them into the apropriate list
	files = os.listdir(base)
	for f in files:
		if f[0] == 'd':
			daily += [datetime.datetime.strptime(f, "d-%Y-%m-%d.%H:%M:%S")]
		elif f[0] == 'w':
			weekly += [datetime.datetime.strptime(f, "w-%Y-%m-%d.%H:%M:%S")]
		elif f[0] == 'm':
			monthly += [datetime.datetime.strptime(f, "m-%Y-%m-%d.%H:%M:%S")]
		elif f == 'current':
			pass
		else:
			warn("Skipped unknown folder '%s'" %f)
	monthly.sort(reverse=True)
	weekly.sort(reverse=True)
	daily.sort(reverse=True)

	# If we have more daily backups than we need offer them to weekly
	if len(daily) > days-1:
		weekly = weekly_from_daily(base, weekly, daily[days-1:])
	else:
		weekly = weekly_from_daily(base, weekly, [])

	# If we have more weekly backups than we need offer them to monthly
	if len(weekly) > weeks:
		monthly = monthly_from_weekly(base, monthly, weekly[weeks:])
	else:
		monthly = monthly_from_weekly(base, monthly, [])

	# Prune of excess months too
	if len(monthly) > months:
		remove_from_monthly(base, monthly[months:])

	# Find the oldest record for doing hardlinks against
	comb = daily[:days] + weekly[:weeks] + monthly[:months]
	comb.sort()
	if len(comb) > 0:
		return comb[-1]
	else:
		return None

def parse_excludes(path,exc):
	ret = []
	exc = exc.strip()

	while len(exc):
		ptr = exc.find("'") 
		if ptr > 0:
			ret += exc[:ptr].strip().split(' ')
			ptr2 = exc[ptr+1:].find("'")
			if ptr < 0:
				raise MyError("PARSE ERROR: cannot parse excludes, non matching \"'\"")
				
			ret += [exc[ptr+1:ptr2+ptr+1].strip()]
			exc = exc[ptr2+ptr+2:].strip()
		else:
			ret += exc.split(' ')
			exc = ''

	# Remove the start from them all
	ret2 = []
	plen = len(path)
	for i in ret:
		if i.startswith(path):
			ret2.append(i[plen:])
		else:
			ret2.append(i)

	return ret2


def parse_comment(config, match):
	return config

def parse_vp(config, match):
	value, pair = match.groups()
	config['VP'][value] = pair
	return config

def parse_class(config, match):
	cls_name, d, w, m, path, exc = match.groups()
	exc = parse_excludes(path,exc)
	config['class'][cls_name] = (path, exc, (int(d),int(w),int(m)))
	return config

def parse_policy1(config, match):
	d, w, m, path, exc = match.groups()
	exc = parse_excludes(path,exc)
	config['policy'][path] = (exc, (int(d),int(w),int(m)))
	return config

def parse_policy2(config, match):
	cls_name, path, exc = match.groups()
	c_path, c_exc, policy = config['class'][cls_name]

	if path == "":
		path = c_path
	if exc == "":
		exc = c_exc
	else:
		exc = parse_excludes(path,exc)

	config['policy'][path] = (exc, policy)
	return config

def parse_file(path):
	re_comment = re.compile("^\s*#.*$|^$")
	re_vp = re.compile("^\s*([A-Z_]+)=(.*)$")
	re_class = re.compile("^\s*class ([a-zA-Z0-9]+)\s*(\d+)\s*(\d+)\s*(\d+)\s*([^\s]*)\s*(.*)$")
	re_policy1 = re.compile("^\s*(\d+)\s*(\d+)\s*(\d+)\s*([^\s]*)\s*(.*)$")
	re_policy2 = re.compile("^\s*@([a-zA-Z0-9]+)\s*([^\s]*)\s*(.*)$")

	re_arr = {re_comment : parse_comment,
				re_vp : parse_vp,
				re_class : parse_class,
				re_policy1 : parse_policy1,
				re_policy2 : parse_policy2}

	config = {'class':{},'VP':{},'policy':{}}

	config['VP']['USER'] = 'root'
	config['VP']['ENABLED'] = 'no'
	config['VP']['RSYNC_OPTIONS'] = ''
	config['VP']['SSH_OPTIONS'] = ''

	fd = open(path, 'r')
	for line in fd:
		for r in re_arr:
			m = r.match(line)
			if m:
				config = re_arr[r](config, m)
				break
		if not m:
			raise MyError("PARSE ERROR: unknown line(%s)" % line)
	fd.close()
	return config
	

def do_host(base,cfg):
	config = parse_file(cfg)
	host_base = base + 'backup/' + config['VP']['HOST']
	
	major(config['VP']['HOST'])

	if 'ENABLED' not in config['VP'] or config['VP']['ENABLED'] == "no":
		minor("Skipping as backup is disabled")
		return

	for d in config['policy']:
		policy_base = host_base+d
		if not os.path.lexists(policy_base):
			os.makedirs(policy_base)
		oldest = clean_and_rotate(policy_base, config['policy'][d][1])
		do_backup(config,base,d, oldest)
	

start = datetime.date(2010,06,25)
jump = datetime.timedelta(days=1)

now = datetime.date.today()
point = start



#while point < now:
#	base = './test'
#	minor(point)
#	do_host(base,'cryptex.rezare.co.nz')
#	#sys.stdin.readline()
#	point += jump
#	#point = now

#sys.stdin.readline()
#days = 7
#weeks = 7
#months = 3
#clean_and_rotate(base)	


major("Backup started")
base = "/backups/"
conf = base + '/configs'

if len(sys.argv) <= 1:
	for i in os.listdir(conf):
		if i.endswith('.cfg'):
			try:
				do_host(base, conf+ '/' + i)
			except:
				error("Caught internal exception:", 1)
				traceback.print_exc()
else:
	for i in sys.argv[1:]:
		try:
			do_host(base, conf+ '/' + i + '.cfg')
		except:
			error("Caught internal exception:", 1)
			traceback.print_exc()
	
major("Backup complete")
if return_code != 0:
	error("Returning with %s" % return_code,0)
	sys.exit(-1)


