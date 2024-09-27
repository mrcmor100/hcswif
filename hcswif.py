#!/usr/bin/env python3

import os
import glob
import re
import sys
import copy
import json
import shutil
import getpass
import argparse
import datetime
import warnings

#------------------------------------------------------------------------------
# Define environment

# Where do you want your job output (json files, stdout, stderr)?

std_out = os.path.join('/farm_out/', getpass.getuser() , 'nps_replay_stdout/')
std_err = os.path.join('/farm_out/', getpass.getuser() , 'nps_replay_stderr/')
json_dir = os.path.join('/group/nps/', getpass.getuser() , 'hcswif/jsons')
tape_out = os.path.join('/mss/hallc/c-nps/analysis/online/replays/')
voli_path = os.path.join('/volatile/hallc/nps/', getpass.getuser())
if not os.path.isdir(std_out):
    warnings.warn('std_out: ' + std_out + ' does not exist')
if not os.path.isdir(std_err):
    warnings.warn('std_err: ' + std_err + ' does not exist')

if not os.path.isdir(json_dir):
    warnings.warn('json_dir: ' + json_dir + ' does not exist')

# Where is your raw data?
sp22_raw_dir = '/mss/hallc/xem2/raw'
sp18_raw_dir = '/mss/hallc/spring17/raw'
sp19_raw_dir = '/mss/hallc/jpsi-007/raw'
cafe_raw_dir = '/mss/hallc/c-cafe-2022/raw'
nps_raw_dir  = '/mss/hallc/c-nps/raw'
if not os.path.isdir(sp22_raw_dir):
    warnings.warn('raw_dir: ' + sp22_raw_dir + ' does not exist')
if not os.path.isdir(sp18_raw_dir):
    warnings.warn('raw_dir: ' + sp18_raw_dir + ' does not exist')
if not os.path.isdir(sp19_raw_dir):
    warnings.warn('raw_dir: ' + sp19_raw_dir + ' does not exist')
if not os.path.isdir(cafe_raw_dir):
    warnings.warn('raw_dir: ' + cafe_raw_dir + ' does not exist')
if not os.path.isdir(nps_raw_dir):
    warnings.warn('raw_dir: ' + nps_raw_dir + ' does not exist')

# Where is hcswif?
hcswif_dir = os.path.dirname(os.path.realpath(__file__))

# hcswif_prefix is used as prefix for workflow, job names, filenames, etc.
now = datetime.datetime.now()
datestr = now.strftime("%Y%m%d%H%M")
hcswif_prefix = 'hcswif' + datestr

#------------------------------------------------------------------------------
# This is the main body of hcswif
def main():
    # Parse the arguments specified by the user
    parsed_args = parseArgs()

    # Turn those arguments into a swif workflow in json format and
    # generate a filename for the json file to write.
    workflow, outfile = getWorkflow(parsed_args)

    # Write the workflow to disk
    writeWorkflow(workflow, outfile)

#------------------------------------------------------------------------------
def parseArgs():
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument('--mode', nargs=1, dest='mode',
            help='type of workflow (replay or command)')
    parser.add_argument('--spectrometer', nargs=1, dest='spectrometer',
                        help='spectrometer to analyze (HMS_ALL, NPS_ALL, HMS_PROD, VLD_REPLAY, NPS_PROD, HMS_COIN, NPS_SKIM, NPS_COIN, NPS_COIN_SCALER, HMS_SCALER, NPS_SCALER)')
    parser.add_argument('--run', nargs='+', dest='run',
            help='a list of run numbers and ranges; or a file listing run numbers')
    parser.add_argument('--events', nargs=1, dest='events',
            help='number of events to analyze (default=all)')
    parser.add_argument('--name', nargs=1, dest='name',
            help='workflow name')
    parser.add_argument('--replay', nargs=1, dest='replay',
            help='hcana replay script; path relative to hallc_replay')
    parser.add_argument('--command', nargs="+", dest='command',
            help='shell command/script to run; or a file containing scripts to run (command mode only)')
    parser.add_argument('--filelist', nargs=1, dest='filelist',
            help='file containing list of files to get from tape (command mode only)')
    parser.add_argument('--account', nargs=1, dest='account',
            help='name of account')
    parser.add_argument('--disk', nargs=1, dest='disk',
            help='disk space in bytes')
    parser.add_argument('--ram', nargs=1, dest='ram',
            help='ram space in bytes')
    parser.add_argument('--cpu', nargs=1, dest='cpu',
            help='cpu cores')
    parser.add_argument('--time', nargs=1, dest='time',
            help='max run time per job in seconds allowed before killing jobs')
    parser.add_argument('--shell', nargs=1, dest='shell',
            help='Currently a shell cannot be specified in SWIF2')
    parser.add_argument('--to_mss', nargs=1, dest='to_mss',
            help='Write the output to mss, default is false')
    parser.add_argument('--all_segs', nargs=1, dest='all_segs',
            help='Add all segments of a run to each job, default is false')
    parser.add_argument('--specify_replay', nargs=1, dest='specify_replay',
            help='Specify the TAR for this nps_replay. Absolute path, default is assumed /group/nps/$USER/nps_replay.tar.gz.')
    parser.add_argument('--constraint', nargs='+', dest='constraint',
            help='user defined SWIF2 constraints (slurm feature).  Space separated if multiple')
    parser.add_argument('--apptainer', nargs=1, dest='apptainer',
                    help='Specify path to apptainer image.')

    print("Ensure your analyzer can compule with the default OS")
    print("Currently no check on constraints.  See the scicomp Slurm Info page for the latest constraints.")
    # Check if any args specified
    if len(sys.argv) < 2:
        raise RuntimeError(parser.print_help())

    # Return parsed arguments
    return parser.parse_args()

#------------------------------------------------------------------------------
def getWorkflow(parsed_args):
    # Initialize
    workflow = initializeWorkflow(parsed_args)
    outfile = os.path.join(json_dir, workflow['name'] + '.json')

    # Get jobs
    if parsed_args.mode==None:
        raise RuntimeError('Must specify a mode (replay or command)')
    mode = parsed_args.mode[0].lower()
    if mode == 'replay':
        workflow['jobs'] = getReplayJobs(parsed_args, workflow['name'])
    elif mode == 'command':
        workflow['jobs'] = getCommandJobs(parsed_args, workflow['name'])
    else:
        raise ValueError('Mode must be replay or command')

    # Add account to jobs
    workflow = addCommonJobInfo(workflow, parsed_args)

    return workflow, outfile

#------------------------------------------------------------------------------
def initializeWorkflow(parsed_args):
    workflow = {}
    if parsed_args.name==None:
        workflow['name'] = hcswif_prefix
    else:
        workflow['name'] = parsed_args.name[0]

    return workflow

#------------------------------------------------------------------------------
def getReplayJobs(parsed_args, wf_name):
    # Spectrometer
    spectrometer = parsed_args.spectrometer[0]
    if spectrometer.upper() not in ['HMS_ALL', 'NPS_ALL', 'VLD_REPLAY', 'HMS_PROD', 'NPS_PROD', 'HMS_COIN', 'NPS_SKIM', 'NPS_COIN', 'NPS_COIN_SCALER', 'HMS_SCALER', 'NPS_SCALER']:
        raise ValueError('Spectrometer must be HMS_ALL, NPS_ALL, VLD_REPLAY, HMS_PROD, NPS_PROD, HMS_COIN, NPS_SKIM, NPS_COIN, NPS_COIN_SCALER, HMS_SCALER, NPS_SCALER')

    # Run(s)
    if parsed_args.run==None:
        raise RuntimeError('Must specify run(s) to process')
    else:
        runs = getReplayRuns(parsed_args.run, parsed_args.disk)

    # Replay script to use
    if parsed_args.replay==None:
        # User has not specified a script, so we provide them with default options
        script_dict = { 'HMS_ALL'        : 'SCRIPTS/HMS/PRODUCTION/replay_production_all_hms.C',
                        'NPS_ALL'       : '',
                        'HMS_PROD'       : 'SCRIPTS/HMS/PRODUCTION/replay_production_hms.C',
                        'NPS_PROD'      : '',
                        'VLD_REPLAY' : 'SCRIPTS/NPS/vld_replay.C', 
                        'HMS_COIN'       : 'SCRIPTS/HMS/PRODUCTION/replay_production_hms_coin.C',
                        'NPS_SKIM'      : 'SCRIPTS/NPS/replay_production_skim_NPS_HMS.C',
                        'NPS_COIN'      : 'SCRIPTS/NPS/replay_production_coin_NPS_HMS.C',
                        'NPS_COIN_SCALER'      : '',
                        'HMS_SCALER'     : 'SCRIPTS/HMS/SCALERS/replay_hms_scalers.C',
                        'NPS_SCALER'    : ''}
        replay_script = script_dict[spectrometer.upper()]
    # User specified a script so we use that one
    else:
        replay_script = parsed_args.replay[0]


    # Number of events; default is -1 (i.e. all)
    if parsed_args.events==None:
        warnings.warn('No events specified. Analyzing all events.')
        evts = -1
    else:
        evts = parsed_args.events[0]

        if parsed_args.all_segs==None:
            all_segs = False
        elif parsed_args.all_segs[0].lower()=='true':
            all_segs = True
            #all_segs = False #Currently don't want things in MSS
        elif parsed_args.all_segs[0].lower()=='false':
            all_segs = False
        else:
            raise RuntimeError('all_segs must be True or False')

    # Which hcswif shell script should we use? bash or csh?
    if parsed_args.shell==None:
        if all_segs==True:
            batch = os.path.join(hcswif_dir, 'hcswif2_all_segs.sh')
        else:
            batch = os.path.join(hcswif_dir, 'hcswif2.sh')
    elif re.search('bash', parsed_args.shell[0]):
        batch = os.path.join(hcswif_dir, 'hcswif2.sh')
    elif re.search('csh', parsed_args.shell[0]):
        batch = os.path.join(hcswif_dir, 'hcswif.csh')
    if parsed_args.apptainer:
        if not os.path.isdir(str(parsed_args.apptainer[0])):
            warnings.warn("APPTAINER image not found.")
            sys.exit()
        batch = os.path.join(hcswif_dir, "hcswif_apptainer.sh")

    # Create list of jobs for workflow
    jobs = []

    for run in runs:
        job = {}

        # Assume coda stem looks like shms_all_XXXXX, hms_all_XXXXX, or coin_all_XXXXX
        if 'coin' in spectrometer.lower():
            # shms_coin and hms_coin use same coda files as coin
            coda_stem = 'nps_coin_' + str(run[0]).zfill(4)
        elif 'all' in spectrometer.lower():
            # otherwise hms_all_XXXXX or shms_all_XXXXX
            all_spec  = spectrometer.replace('_ALL', '')
            coda_stem = 'nps_coin_' + str(run[0]).zfill(4)
        elif 'prod' in spectrometer.lower():
            # otherwise hms_all_XXXXX or shms_all_XXXXX
            prod_spec = spectrometer.replace('_PROD', '')
            coda_stem = 'nps_coin_' + str(run[0]).zfill(4)
        elif 'scaler' in spectrometer.lower():
            # otherwise hms_all_XXXXX or shms_all_XXXXX
            scaler_spec = spectrometer.replace('_SCALER', '')
            coda_stem = 'nps_coin_' + str(run[0]).zfill(4)
        else:
            # otherwise hms_all_XXXXX or shms_all_XXXXX
            coda_stem = 'nps_coin_' + str(run[0]).zfill(4)

#        if (run[0] > 11000 and 'shms' in spectrometer.lower()) : 
#           coda = os.path.join(sp22_raw_dir, coda_stem + '.dat')
#        elif (run[0] > 4000 and 'hms' in spectrometer.lower()) : 
#           coda = os.path.join(sp22_raw_dir, coda_stem + '.dat')
        #if(run[0]>4300 and run[0] < 16940 and 'hms' in spectrometer.lower()):
            #coda = os.path.join(sp22_raw_dir, coda_stem + '.dat')
        #elif (run[0] > 16940 and run[0] < 17144 and 'hms' in spectrometer.lower()) : 
            #These are all COIN run[0]s.
            #coda_stem = 'shms_all_' + str(run[0]).zfill(5)
            #coda = os.path.join(cafe_raw_dir, coda_stem + '.dat')
          
            #TODO: Add option to run all segements of a run as output.
        #if parsed_args.all_segments==None:
        # Check if raw data file exist
        coda = os.path.join(nps_raw_dir, coda_stem + '.dat.' + str(run[2]))
        coda0 = os.path.join(nps_raw_dir, coda_stem + '.dat.0')
        if not os.path.isfile(coda):
            warnings.warn('RAW DATA: ' + coda + ' does not exist.')
        if not os.path.isfile(coda0):
            warnings.warn('RAW DATA: ' + coda0 + ' does not exist.')
            #continue

            #TODO: Fix collision between HMS rootfile names
        output_dict = { 'HMS_ALL'             : 'ROOTfiles/hms_replay_production_all_%d_%d_%d.root',
                        'NPS_ALL'                : '',
                        'HMS_PROD'               : 'ROOTfiles/HMS/PRODUCTION/hms_replay_production_%d_%d_%d.root',
                        'NPS_PROD'             : '',
                        'VLD_REPLAY' : 'ROOTfiles/nps_%d.root',
                        'HMS_COIN'       : 'ROOTfiles/HMS/PRODUCTION/hms_replay_production_%d_%d_%d.root',
                        'NPS_SKIM'      : 'ROOTfiles/COIN/SKIM/nps_hms_skim_%d_%d_%d.root',
                        'NPS_COIN'      : 'ROOTfiles/COIN/PRODUCTION/nps_hms_coin_%d_%d_1_%d.root',
                        'NPS_COIN_SCALER' : '',
                        'HMS_SCALER'           : 'ROOTfiles/HMS/SCALARS/hms_replay_scalars_%d_%d_%d.root',
                        'NPS_SCALER'          : ''}
        script_output = output_dict[spectrometer.upper()]

        #No output tape path determined yet.
        output_path_dict = { 'HMS_ALL'    : '',
                             'NPS_ALL'                : '',
                             'HMS_PROD'               : '',
                             'NPS_PROD'             : '',
                             'VLD_REPLAY' : '',
                             'HMS_COIN'               : '',
                             'NPS_SKIM'             : 'production/',
                             'NPS_COIN'             : '',
                             'NPS_COIN_SCALER' : '',
                             'HMS_SCALER'            : '',
                             'NPS_SCALER'          : ''}
        output_path = output_path_dict[spectrometer.upper()]

        if parsed_args.specify_replay==None:
            specify_replay=os.path.join('/group/nps/', getpass.getuser() , 'nps_replay.tar.gz')
            if not os.path.isfile(specify_replay):
                raise ValueError('No default replay TAR found.')       
        else:
            specify_replay=os.path.join(parsed_args.specify_replay[0])
            if not os.path.isfile(specify_replay):
                raise ValueError('User defined replay path and TAR must be valid.')       
                
        if parsed_args.to_mss==None:
            to_mss = False
        elif parsed_args.to_mss[0].lower()=='true':
            to_mss = True
            #to_mss = False #Currently don't want things in MSS
        elif parsed_args.to_mss[0].lower()=='false':
            to_mss = False
        else:
            raise RuntimeError('to_mss must be True or False')

        job['name'] =  wf_name + '_' + coda_stem + '.dat.' + str(run[2])
        job['constraint'] = processConstraints(parsed_args.constraint)
        job['name'] =  wf_name + '_' + coda_stem
        job['inputs'] = [{}]
        job['inputs'][0]['local'] = "nps_replay.tar.gz"
        job['inputs'][0]['remote'] = specify_replay

        #Running sum of disk space required, not ideal. Start off with 1GB (for replay)
        tmp_disk=1000000000
        if all_segs==True:
            last_seg = run[2]+1
            first_seg = 0
            tmp_disk=20000000000*run[2] + 30000000000
            for seg in range(first_seg,last_seg):
                coda = os.path.join(nps_raw_dir, coda_stem + '.dat.' + str(seg))
                if not os.path.isfile(coda):
                    warnings.warn('RAW DATA: ' + coda + ' does not exist.')
                inp={}
                inp['local'] = os.path.basename(coda)
                inp['remote'] = coda
                job['inputs'].append(inp)
        else:
            #Specify file size as 20GB by hand, not ideal.
            tmp_disk+=int(20000000000)
            job['inputs'].append({})
            job['inputs'][1]['local'] = os.path.basename(coda0)
            job['inputs'][1]['remote'] = coda0
            #print(coda0)
            job['inputs'].append({})
            job['inputs'][2]['local'] = os.path.basename(coda)
            job['inputs'][2]['remote'] = coda
            tmp_disk+=int(20000000000)
            #print(coda)
        if to_mss:
            #DOES NOT WORK FOR EVERYTHING!!!
            job['outputs'] = [{}]
            if spectrometer.upper() == 'NPS_SKIM':
                job['outputs'][0]['local'] = script_output % (int(run[0]), int(1), int(-1))
                job['outputs'][0]['remote'] = tape_out + output_path + (os.path.basename(script_output % (int(run[0]), int(1), int(-1))))
            else:
                job['outputs'][0]['local'] = script_output % (int(run[0]), int(run[2]), int(evts))
                job['outputs'][0]['remote'] = tape_out + output_path + (os.path.basename(script_output % (int(run[0]), int(run[2]), int(evts))))
        #This is for replay of all segments.
        #job['disk_bytes'] = 
        #This is for segment jobs.
        #job['disk_bytes'] = 2*run[1] + 30000000000
        job['disk_bytes'] = tmp_disk
        #if spectrometer.upper()=='NPS_PROD':
            #job['time_secs'] = int((run[2] / 6000 / 75)*1.2)
        #elif spectrometer.upper()=='NPS_SCALER':
            #job['time_secs'] = int((run[2] / 6000 / 500)*1.1)
            

        # command for job is `/hcswifdir/hcswif.sh REPLAY RUN NUMEVENTS`
        if parsed_args.apptainer:
             job['command'] = [" ".join([batch, replay_script, str(run), str(evts), str(parsed_args.apptainer[0]), str(raw_dir)])]
        else:
          job['command'] = [" ".join([batch, replay_script, str(run[0]), str(evts), str(run[2])])]
     
        jobs.append(copy.deepcopy(job))

    return jobs

#------------------------------------------------------------------------------
def getReplayRuns(run_args, disk_args):
    runs = []
    disk = []
    seg = []
    antecedent = []
    # User specified a file containing runs
    if (run_args[0]=='file'):
        filelist = run_args[1]
        f = open(filelist,'r')
        lines = f.readlines()

        # We assume user has been smart enough to only specify valid run numbers
        # or, at worst, lines only containing a \n
        for line in lines:
            splitted = line.split(" ")
            if len(splitted) > 1:
                run = splitted[0]
                seg = splitted[1]
                disk = splitted[2]
            else:
                run = line.strip('\n')
                if disk_args==None:
                    disk_bytes = 10000000000
                    disk = int(disk_bytes)
                else:
                    disk = int(disk_args[0])
                seg=''
            if len(run)>0:
                runs.append([int(run), int(disk), int(seg)])

    # Arguments are either individual runs or ranges of runs. We check with a regex
    else:
        for arg in run_args:
            # Is it a range? e.g. 2040-2055
            if re.match('^\d+-\d+$', arg):
                limits = re.split(r'-', arg)
                first = int(limits[0])
                last = int(limits[1]) + 1 # range(n,m) stops at m-1

                for run in range(first, last):
                    runs.append(run)

            # Is it a single run? e.g. 2049
            elif re.match('^\d+$', arg):
                runs.append(int(arg))

            # Else, invalid argument so we warn and skip it
            else:
                warnings.warn('Invalid run argument: ' + arg)

    return runs

#------------------------------------------------------------------------------
def processConstraints(swif2_constraints):
    #Constraints will come in an array if entered with space separations.
    if swif2_constraints == None:
        return "el9"
    else:
        return ','.join(swif2_constraints)
    return "el9"


    #------------------------------------------------------------------------------
def getCommandJobs(parsed_args, wf_name):
    print("Broken for non-PATTERN filelist input...\n See code!")
    # command for job should have been specified by user
    if parsed_args.command==None:
        raise RuntimeError('Must specify command for batch job')

    jobs = []
    commands = []

    # User specified a text file containing commands
    if (parsed_args.command[0]=='file'):
        filelist = parsed_args.command[1]
        f = open(filelist,'r')
        lines = f.readlines()

        # We assume user has been smart enough to only specify valid commands
        # or, at worst, lines only containing a \n
        for line in lines:
            cmd = line.strip('\n')
            if len(cmd)>0:
                commands.append(cmd)

    # Otherwise user only specified one command, which may or may not have arguments.
    # join() works in either case.
    else:
        cmd = ' '.join(str(element) for element in parsed_args.command)
        commands.append(cmd)

    for cmd in commands:
        job = {}
        job['name'] = wf_name + '_job' + str(len(jobs))

        job['command'] = [cmd]

        # Add any necessary files from tape
        if parsed_args.filelist==None:
            warnings.warn('No file list specified. Assuming your shell script has any necessary jgets')
        else:
            filelist = parsed_args.filelist[0]
            f = open(filelist,'r')
            lines = f.readlines()
            line0 = lines[0].split(" ")
            lines = lines[1:]
            if('PATTERN' in line0[0]):
                # Broken for non-PATTERN filelist input...
                # Which parameter of the command would we like to pattern off of?
                parameter = line0[1] # Typically this is the run number.
                cmd_options = cmd.split(" ")
                param = cmd_options[int(parameter)]
            # We assume user has been smart enough to only specify valid files
            # or, at worst, lines only containing a \n
            job['inputs'] = []
            for line in lines:
                filename = line.strip('\n')
                filename = filename.format(param=param)
                if len(filename)>0:
                    if not os.path.isfile(filename):
                        warnings.warn('RAW DATA: ' + filename + ' does not exist')
                    inp={}
                    inp['local'] = os.path.basename(filename)
                    inp['remote'] = filename
                    job['inputs'].append(inp)

        jobs.append(copy.deepcopy(job))

    return jobs

#------------------------------------------------------------------------------
def addCommonJobInfo(workflow, parsed_args):
    # Account
    # TODO: Remove default?
    if parsed_args.account==None:
        warnings.warn('No account specified.')

        account_prompt = 'x'
        while account_prompt.lower() not in ['y', 'n', 'yes', 'no']:
            account_prompt = input('Should I use account=hallc? (y/n): ')

        if account_prompt.lower() in ['y', 'yes']:
            account = 'hallc'
        else:
            raise RuntimeError('Please specify account as argument')
    else:
        account = parsed_args.account[0]

    # Disk space in bytes
    if parsed_args.disk==None:
        disk_bytes = 10000000000
    else:
        disk_bytes = int(parsed_args.disk[0])

    # RAM in bytes
    if parsed_args.ram==None:
        ram_bytes = 2500000000
    else:
        ram_bytes = int(parsed_args.ram[0])

    # CPUs
    if parsed_args.cpu==None:
        cpu = 1
    else:
        cpu = int(parsed_args.cpu[0])

    # Max time in seconds before killing jobs
    if parsed_args.time==None:
        time = 14400
    else:
        time = int(parsed_args.time[0])

    # Shell
    if parsed_args.shell==None:
        shell = shutil.which('bash')
    else:
        shell = shutil.which(parsed_args.shell[0])

    # Loop over jobs and add info
    for n in range(0, len(workflow['jobs'])):
        job = copy.deepcopy(workflow['jobs'][n])

        job['account'] = account

        job['stdout'] = os.path.join(std_out, job['name'] + '.out')
        job['stderr'] = os.path.join(std_err, job['name'] + '.err')

        # TODO: Allow user to specify all of these parameters
        job['constraint'] = processConstraints(parsed_args.constraint)
        job['partition'] = 'production'
        #job['disk_bytes'] = disk_bytes
        job['ram_bytes'] = ram_bytes
        job['cpu_cores'] = cpu
        if parsed_args.time!=None:
            job['time_secs'] = time

        workflow['jobs'][n] = copy.deepcopy(job)
        job.clear()

    return workflow

#------------------------------------------------------------------------------
def writeWorkflow(workflow, outfile):
    with open(outfile, 'w') as f:
        json.dump(workflow, f, sort_keys=True, indent=2, separators=(',', ': '))

    print('Wrote: ' + outfile)
    return

#------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
