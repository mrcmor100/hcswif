#!/usr/bin/env python3
"""
Refactored hcswif workflow creator

This version extracts hard-coded parameters into a configuration
and organizes the code into more modular functions.
"""

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

#--------------------------------------------------------------------
# Configuration Module
# In practice, this might be in a separate file (e.g. config.py)
# and imported with "import config". For now it is inline.

class Config:
    # User and time-dependent parameters
    USER = getpass.getuser()
    NOW = datetime.datetime.now()
    DATESTR = NOW.strftime("%Y%m%d%H%M")
    HCSWIF_PREFIX = 'hcswif' + DATESTR

    # Directories for job outputs and workflow files
    BASE_DIRS = {
        "std_out": os.path.join('./farm_out/', USER, 'replay_stdout'),
        "std_err": os.path.join('./farm_out/', USER, 'replay_stderr'),
        "json_dir": os.path.join('./', 'hcswif/jsons'),
        "tape_out": os.path.join('./mss/replays'),
        "voli_path": os.path.join('./volatile/', USER)
    }

    # Directories for raw data
    RAW_DIRS = {
        "sp22": '/mss/hallc/xem2/raw',
        "sp18": '/mss/hallc/spring17/raw',
        "sp19": '/mss/hallc/jpsi-007/raw',
        "cafe": '/mss/hallc/c-cafe-2022/raw',
        "nps": '/mss/hallc/c-nps/raw'
    }

    # Default values for job resources
    DEFAULTS = {
        "events": -1,             # -1 means all events
        "disk_bytes": 10000000000,
        "ram_bytes": 2500000000,
        "cpu": 1,
        "time_secs": 14400,
        "constraint": "el9",
        "partition": "production"
    }

    # Default replay scripts for each spectrometer type.
    REPLAY_SCRIPTS = {
        'HMS_ALL': 'SCRIPTS/HMS/PRODUCTION/replay_production_all_hms.C',
        'NPS_ALL': '',
        'HMS_PROD': 'SCRIPTS/HMS/PRODUCTION/replay_production_hms.C',
        'NPS_PROD': '',
        'VLD_REPLAY': 'SCRIPTS/NPS/vld_replay.C',
        'HMS_COIN': 'SCRIPTS/HMS/PRODUCTION/replay_production_hms_coin.C',
        'NPS_SKIM': 'SCRIPTS/NPS/replay_production_skim_NPS_HMS.C',
        'NPS_COIN': 'SCRIPTS/NPS/replay_production_coin_NPS_HMS.C',
        'NPS_COIN_SCALER': '',
        'HMS_SCALER': 'SCRIPTS/HMS/SCALERS/replay_hms_scalers.C',
        'NPS_SCALER': ''
    }

    # Output filename formats for each spectrometer type
    OUTPUT_FORMATS = {
        'HMS_ALL': 'ROOTfiles/hms_replay_production_all_%d_%d_%d.root',
        'NPS_ALL': '',
        'HMS_PROD': 'ROOTfiles/HMS/PRODUCTION/hms_replay_production_%d_%d_%d.root',
        'NPS_PROD': '',
        'VLD_REPLAY': 'ROOTfiles/nps_%d.root',
        'HMS_COIN': 'ROOTfiles/HMS/PRODUCTION/hms_replay_production_%d_%d_%d.root',
        'NPS_SKIM': 'ROOTfiles/COIN/SKIM/nps_hms_skim_%d_%d_%d.root',
        'NPS_COIN': 'ROOTfiles/COIN/PRODUCTION/nps_hms_coin_%d_%d_1_%d.root',
        'NPS_COIN_SCALER': '',
        'HMS_SCALER': 'ROOTfiles/HMS/SCALARS/hms_replay_scalars_%d_%d_%d.root',
        'NPS_SCALER': ''
    }

    # Additional output path suffixes for specific spectrometer types
    OUTPUT_PATHS = {
        'HMS_ALL': '',
        'NPS_ALL': '',
        'HMS_PROD': '',
        'NPS_PROD': '',
        'VLD_REPLAY': '',
        'HMS_COIN': '',
        'NPS_SKIM': 'production/',
        'NPS_COIN': '',
        'NPS_COIN_SCALER': '',
        'HMS_SCALER': '',
        'NPS_SCALER': ''
    }

    # Disk and MSS configuration
    DISK_SPECS = {
        "base_disk": 1000000000,             # base disk space (in bytes)
        "non_all_segs": 20000000000,         # disk space increment for non-all_segs jobs
        "all_segs_multiplier": 20000000000,  # multiplier per segment when all segments are processed
        "all_segs_offset": 30000000000       # additional offset when all segments are processed
    }
    TO_MSS_DEFAULT = False  # default flag for writing outputs to MSS

    # Default path for the replay tar file
    DEFAULT_REPLAY_TAR = os.path.join('/group/nps/', USER, 'nps_replay.tar.gz')


#--------------------------------------------------------------------
# Utility functions
def check_directories():
    """Check that key directories exist; warn if they do not."""
    for key, path in Config.BASE_DIRS.items():
        if not os.path.isdir(path):
            warnings.warn(f"{key} directory does not exist: {path}")
    for key, path in Config.RAW_DIRS.items():
        if not os.path.isdir(path):
            warnings.warn(f"Raw data directory for {key} does not exist: {path}")


#--------------------------------------------------------------------
# Argument parsing remains largely the same
def parseArgs():
    parser = argparse.ArgumentParser()

    # Add arguments; these override configuration defaults if provided.
    parser.add_argument('--mode', nargs=1, dest='mode',
                        help='type of workflow (replay or command)')
    parser.add_argument('--spectrometer', nargs=1, dest='spectrometer',
                        help='spectrometer to analyze (HMS_ALL, NPS_ALL, etc.)')
    parser.add_argument('--run', nargs='+', dest='run',
                        help='a list of run numbers and ranges; or a file listing run numbers')
    parser.add_argument('--events', nargs=1, dest='events',
                        help='number of events to analyze (default=all)')
    parser.add_argument('--name', nargs=1, dest='name',
                        help='workflow name')
    parser.add_argument('--replay', nargs=1, dest='replay',
                        help='hcana replay script; path relative to hallc_replay')
    parser.add_argument('--command', nargs="+", dest='command',
                        help='shell command/script to run; or a file containing scripts (command mode only)')
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
                        help='Specify the TAR for this nps_replay. Absolute path, default is assumed in config.')
    parser.add_argument('--constraint', nargs='+', dest='constraint',
                        help='user defined SWIF2 constraints (slurm feature).  Space separated if multiple')
    parser.add_argument('--apptainer', nargs=1, dest='apptainer',
                        help='Specify path to apptainer image.')

    # Informational output
    print("Ensure your analyzer can compile with the default OS")
    print("No check on constraints; see the scicomp Slurm Info page for the latest constraints.")

    if len(sys.argv) < 2:
        raise RuntimeError(parser.print_help())
    return parser.parse_args()


#--------------------------------------------------------------------
# Workflow creation functions
def initializeWorkflow(parsed_args):
    workflow = {}
    if parsed_args.name is None:
        workflow['name'] = Config.HCSWIF_PREFIX
    else:
        workflow['name'] = parsed_args.name[0]
    return workflow


def addCommonJobInfo(workflow, parsed_args):
    # Set account
    if parsed_args.account is None:
        warnings.warn('No account specified.')
        # Prompt user interactively if needed
        account_prompt = input('Should I use account=hallc? (y/n): ')
        if account_prompt.lower() in ['y', 'yes']:
            account = 'hallc'
        else:
            raise RuntimeError('Please specify account as argument')
    else:
        account = parsed_args.account[0]

    # Use configuration defaults if arguments are not provided
    disk_bytes = int(parsed_args.disk[0]) if parsed_args.disk else Config.DEFAULTS['disk_bytes']
    ram_bytes = int(parsed_args.ram[0]) if parsed_args.ram else Config.DEFAULTS['ram_bytes']
    cpu = int(parsed_args.cpu[0]) if parsed_args.cpu else Config.DEFAULTS['cpu']
    time_secs = int(parsed_args.time[0]) if parsed_args.time else Config.DEFAULTS['time_secs']
    shell = shutil.which(parsed_args.shell[0]) if parsed_args.shell else shutil.which('bash')

    for n, job in enumerate(workflow['jobs']):
        job['account'] = account
        job['stdout'] = os.path.join(Config.BASE_DIRS['std_out'], job['name'] + '.out')
        job['stderr'] = os.path.join(Config.BASE_DIRS['std_err'], job['name'] + '.err')
        job['constraint'] = processConstraints(parsed_args.constraint)
        job['partition'] = Config.DEFAULTS['partition']
        job['ram_bytes'] = ram_bytes
        job['cpu_cores'] = cpu
        if parsed_args.time:
            job['time_secs'] = time_secs
        workflow['jobs'][n] = copy.deepcopy(job)
    return workflow


def getWorkflow(parsed_args):
    workflow = initializeWorkflow(parsed_args)
    outfile = os.path.join(Config.BASE_DIRS['json_dir'], workflow['name'] + '.json')
    mode = parsed_args.mode[0].lower() if parsed_args.mode else None

    if mode == 'replay':
        workflow['jobs'] = getReplayJobs(parsed_args, workflow['name'])
    elif mode == 'command':
        workflow['jobs'] = getCommandJobs(parsed_args, workflow['name'])
    else:
        raise ValueError('Mode must be replay or command')

    workflow = addCommonJobInfo(workflow, parsed_args)
    return workflow, outfile


def processConstraints(constraints):
    if constraints is None:
        return Config.DEFAULTS['constraint']
    else:
        return ','.join(constraints)


#--------------------------------------------------------------------
# New helper function to compute disk space and set up input files for raw data
def prepare_raw_inputs(raw_dir, coda_stem, run, all_segs):
    """
    Computes required disk space and builds input file entries for raw data.
    
    Returns:
        inputs: List of input file dictionaries.
        disk_bytes: Total disk space (in bytes) estimated.
    """
    inputs = []
    disk = Config.DISK_SPECS["base_disk"]
    
    if all_segs:
        last_seg = run[2] + 1
        # Disk calculation: base multiplier times number of segments plus an offset
        disk = Config.DISK_SPECS["all_segs_multiplier"] * run[2] + Config.DISK_SPECS["all_segs_offset"]
        for seg in range(0, last_seg):
            seg_file = os.path.join(raw_dir, f"{coda_stem}.dat.{seg}")
            if not os.path.isfile(seg_file):
                warnings.warn('RAW DATA missing: ' + seg_file)
            inputs.append({
                'local': os.path.basename(seg_file),
                'remote': seg_file
            })
    else:
        # For non-all_segs mode, add two specific segments (0 and the current seg)
        disk += Config.DISK_SPECS["non_all_segs"]
        coda0 = os.path.join(raw_dir, f"{coda_stem}.dat.0")
        coda  = os.path.join(raw_dir, f"{coda_stem}.dat.{run[2]}")
        inputs.append({
            'local': os.path.basename(coda0),
            'remote': coda0
        })
        inputs.append({
            'local': os.path.basename(coda),
            'remote': coda
        })
        disk += Config.DISK_SPECS["non_all_segs"]
    return inputs, disk

#--------------------------------------------------------------------
# Updated getReplayJobs function
def getReplayJobs(parsed_args, wf_name):
    # Validate spectrometer option
    spectrometer = parsed_args.spectrometer[0]
    valid_specs = list(Config.REPLAY_SCRIPTS.keys())
    if spectrometer.upper() not in valid_specs:
        raise ValueError('Spectrometer must be one of: ' + ', '.join(valid_specs))
    
    # Process run numbers
    if parsed_args.run is None:
        raise RuntimeError('Must specify run(s) to process')
    runs = getReplayRuns(parsed_args.run, parsed_args.disk)
    
    # Choose replay script (either user-specified or default)
    replay_script = parsed_args.replay[0] if parsed_args.replay else Config.REPLAY_SCRIPTS[spectrometer.upper()]
    
    # Number of events
    if parsed_args.events is None:
        warnings.warn('No events specified. Analyzing all events.')
        evts = Config.DEFAULTS['events']
    else:
        evts = parsed_args.events[0]
    
    # Determine all_segs flag
    if parsed_args.all_segs is None:
        all_segs = False
    elif parsed_args.all_segs[0].lower() in ['true']:
        all_segs = True
    elif parsed_args.all_segs[0].lower() in ['false']:
        all_segs = False
    else:
        raise RuntimeError('all_segs must be True or False')
    
    # Select the proper batch script based on shell and all_segs options
    hcswif_dir = os.path.dirname(os.path.realpath(__file__))
    if parsed_args.shell is None:
        batch = os.path.join(hcswif_dir, 'hcswif2_all_segs.sh' if all_segs else 'hcswif2.sh')
    elif re.search('bash', parsed_args.shell[0]):
        batch = os.path.join(hcswif_dir, 'hcswif2.sh')
    elif re.search('csh', parsed_args.shell[0]):
        batch = os.path.join(hcswif_dir, 'hcswif.csh')
    else:
        batch = os.path.join(hcswif_dir, 'hcswif2.sh')
    
    if parsed_args.apptainer:
        if not os.path.isdir(str(parsed_args.apptainer[0])):
            warnings.warn("APPTAINER image not found.")
            sys.exit(1)
        batch = os.path.join(hcswif_dir, "hcswif_apptainer.sh")
    
    jobs = []
    raw_dir = Config.RAW_DIRS['nps']
    
    for run in runs:
        job = {}
        # Build coda stem; customize as needed for different spectrometer types
        coda_stem = 'nps_coin_' + str(run[0]).zfill(4)
        
        # Prepare raw data inputs and disk space calculation via helper function
        inputs, disk_bytes = prepare_raw_inputs(raw_dir, coda_stem, run, all_segs)
        job['inputs'] = [{
            'local': "nps_replay.tar.gz",
            'remote': (parsed_args.specify_replay[0]
                       if parsed_args.specify_replay
                       else Config.DEFAULT_REPLAY_TAR)
        }]
        # Append raw data inputs
        job['inputs'].extend(inputs)
        job['disk_bytes'] = disk_bytes
        
        # Configure output formatting if writing to MSS
        # The output format and path are defined in the Config section
        script_output = Config.OUTPUT_FORMATS[spectrometer.upper()]
        output_path = Config.OUTPUT_PATHS[spectrometer.upper()]
        if parsed_args.to_mss is None:
            to_mss = Config.TO_MSS_DEFAULT
        elif parsed_args.to_mss[0].lower() in ['true']:
            to_mss = True
        elif parsed_args.to_mss[0].lower() in ['false']:
            to_mss = False
        else:
            raise RuntimeError('to_mss must be True or False')
        
        if to_mss:
            job['outputs'] = [{}]
            if spectrometer.upper() == 'NPS_SKIM':
                local_name = script_output % (int(run[0]), 1, -1)
            else:
                local_name = script_output % (int(run[0]), int(run[2]), int(evts))
            job['outputs'][0]['local'] = local_name
            job['outputs'][0]['remote'] = os.path.join(Config.BASE_DIRS['tape_out'], output_path, os.path.basename(local_name))
        
        # Create a unique job name
        job['name'] = wf_name + '_' + coda_stem
        job['constraint'] = processConstraints(parsed_args.constraint)
        
        # Construct the job command
        if parsed_args.apptainer:
            job['command'] = [" ".join([batch, replay_script, str(run), str(evts),
                                         str(parsed_args.apptainer[0]), str(raw_dir)])]
        else:
            job['command'] = [" ".join([batch, replay_script, str(run[0]), str(evts), str(run[2])])]
        
        jobs.append(copy.deepcopy(job))
    
    return jobs

def getReplayRuns(run_args, disk_args):
    """Parses run arguments which can be either a list of runs/ranges or a file input."""
    runs = []
    # If the user provided a file, the first argument is 'file'
    if run_args[0] == 'file':
        filelist = run_args[1]
        with open(filelist, 'r') as f:
            lines = f.readlines()
        for line in lines:
            splitted = line.split()
            if len(splitted) > 1:
                run = splitted[0]
                seg = splitted[1]
                disk = splitted[2]
            else:
                run = line.strip()
                disk = disk_args[0] if disk_args else Config.DEFAULTS['disk_bytes']
                seg = ''
            if run:
                runs.append([int(run), int(disk), int(seg)])
    else:
        for arg in run_args:
            if re.match('^\d+-\d+$', arg):
                first, last = map(int, arg.split('-'))
                for run in range(first, last + 1):
                    # For range jobs we assume a default segment value of 0
                    runs.append([run, Config.DEFAULTS['disk_bytes'], 0])
            elif re.match('^\d+$', arg):
                runs.append([int(arg), Config.DEFAULTS['disk_bytes'], 0])
            else:
                warnings.warn('Invalid run argument: ' + arg)
    return runs


#--------------------------------------------------------------------
# Command job functions (refactored similarly)
def getCommandJobs(parsed_args, wf_name):
    print("Command mode is not fully implemented. Please see the code!")
    if parsed_args.command is None:
        raise RuntimeError('Must specify command for batch job')

    jobs = []
    commands = []
    if parsed_args.command[0] == 'file':
        filelist = parsed_args.command[1]
        with open(filelist, 'r') as f:
            lines = f.readlines()
        for line in lines:
            cmd = line.strip()
            if cmd:
                commands.append(cmd)
    else:
        commands.append(' '.join(parsed_args.command))

    for cmd in commands:
        job = {}
        job['name'] = wf_name + '_job' + str(len(jobs))
        job['command'] = [cmd]
        # Process file inputs if specified
        if parsed_args.filelist is None:
            warnings.warn('No file list specified. Assuming your shell script handles necessary file transfers.')
        else:
            filelist = parsed_args.filelist[0]
            with open(filelist, 'r') as f:
                lines = f.readlines()
            # Example: if the file list has a header with PATTERN information.
            if lines:
                header = lines[0].split()
                if 'PATTERN' in header[0]:
                    parameter = header[1]
                    cmd_options = cmd.split()
                    param = cmd_options[int(parameter)]
                else:
                    param = None
            job['inputs'] = []
            for line in lines[1:]:
                filename = line.strip()
                if param:
                    filename = filename.format(param=param)
                if filename and not os.path.isfile(filename):
                    warnings.warn('RAW DATA: ' + filename + ' does not exist')
                job['inputs'].append({
                    'local': os.path.basename(filename),
                    'remote': filename
                })
        jobs.append(copy.deepcopy(job))
    return jobs


def writeWorkflow(workflow, outfile):
    with open(outfile, 'w') as f:
        json.dump(workflow, f, sort_keys=True, indent=2, separators=(',', ': '))
    print('Wrote workflow to:', outfile)


#--------------------------------------------------------------------
def main():
    # Only check directories if the help flag is not present.
    if not any(flag in sys.argv for flag in ['-h', '--help']):
        check_directories()
    parsed_args = parseArgs()
    workflow, outfile = getWorkflow(parsed_args)
    writeWorkflow(workflow, outfile)

if __name__ == "__main__":
    main()
