#!/usr/bin/python
###################Brief description###############################
# MultiProcess.py
# A python script to execute a list of 
# multiple processes simultaneously 

###################FreeBSD license header##########################
# Copyright (c) 2012, Wilston Oreo
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met: 
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer. 
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution. 
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
###################################################################

import os, sys, time

from csv import reader
from shlex import split
from subprocess import Popen

def printUsage():
	print "MultiCore.py [NUMBER_OF_PROCESSES] Joblist1.txt Joblist2.txt ...\n"
	print "A job list is a CSV with semicolon separated columns."
	print "A line of a job list has the following syntax:"
	print "JOBID;JOBCMD;JOBLOGFILE"
	exit()

######################Additional parameters########################

N_PROCESSES = 4 # Default number of processes
LOGLEVEL 	= 2 # Verbosity of log output

###################################################################

def log(text,loglevel,msgtype):
	if loglevel > LOGLEVEL: return
	timeStr = time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime())
	print "%s|%s: %s" % (timeStr, msgtype, text)

# Load the job list given in the command line parameter
# and return the number of processes execute in parallel
def loadJobLists(jobList):
	jobIndex, nProcesses = 0, N_PROCESSES

	# Get all job lists from command line
	for jobListFilename in sys.argv[1:]:

		# Command line argument is a number, set it as number of processes
		if jobListFilename.isdigit(): 
			nProcesses = int(jobListFilename)
			continue
		
		with open(jobListFilename,"rb") as f: 
			jobListCSV = reader(f, delimiter=';', quotechar='"')

			for job in jobListCSV:
				# Continue if there more or less than three elements in that row
				if len(job)!=3: continue

				# Get the jobId
				jobId = str(jobIndex)
				if job[0].strip()!="": jobId = job[0].strip()

				# Get log file
				logFile = jobId+".log"
				if job[2].strip()!="": logFile = job[2].strip()

				# Append job
				jobList.append((jobId,jobListFilename,job[1].strip(),logFile))
				jobIndex += 1
			f.close()

	return nProcesses

def runJobs(jobList,nProcesses):
	def addProcessToQueue(jobId,jobListFile,cmd,logFile):
		try:
			jobarglist = split(cmd)
			log("Job '%s' from job list '%s' was started!" % (jobId,jobListFile),1,"MSG")
			
			if logFile=="":
				logFileObj = None
				process = Popen(jobarglist,stdout=None)
			else:
				logFileObj = open(logFile,"w")
				process = Popen(jobarglist,stdout=logFileObj)
				log("Output of job '%s' will be written to '%s'." % (jobId ,logFile),2,"MSG")

			log("Job command: %s" % cmd,3,"MSG")
			processList.append((process,cmd,logFile,logFileObj))
		except:
			log("Could not add process '%s'!" % jobId,0,"ERR")

	def countActiveProcesses():
		count = 0
		for element in processList:
			(process,job,logfile,logfileobj) = element
			if process.poll() == None: count += 1
			else:
				if logfile!="": logfileobj.close()
		return count
	
	def jobListInfo():
		nJobsRunning = countActiveProcesses()+1
		nJobsToStart = len(jobsToStart)
		nJobsStarted = len(jobList) - len(jobsToStart)
		return "%d jobs running, %d to start, %d jobs started." % (nJobsRunning,nJobsToStart,nJobsStarted)

	jobsToStart = list(jobList)
	jobsToStart.reverse()
	processList = []

	while len(jobsToStart)>0:
		# While loop to keep N_PROCESSES continously running
		while countActiveProcesses() < nProcesses: 
			if len(jobsToStart)==0: break
			log(jobListInfo(),1,"MSG")

			(jobId,jobListFile,cmd,logFile) = jobsToStart.pop()			
			addProcessToQueue(jobId,jobListFile,cmd,logFile)
		
		# Close open log files
		for element in processList:
			(process,job,logfile,logfileobj) = element
			if logfile!="": logfileobj.close()
	
	if len(jobsToStart)>len(jobList): log(jobListInfo(),1,"MSG")
	log("All jobs started.",2,"MSG")

# the main function
if __name__ == "__main__":
	# Check command line arguments 
	if (len(sys.argv)<2): printUsage()

	jobs = []
	nProcesses = loadJobLists(jobs)
	runJobs(jobs, nProcesses)
