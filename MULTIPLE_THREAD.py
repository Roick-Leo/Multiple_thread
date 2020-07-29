import time
import subprocess
import argparse


###input parameters
parser = argparse.ArgumentParser(description='Running multiple commands with multiple threads.')

parser.add_argument('-T', '--threads', default=8, type=int, help='number of threads')
parser.add_argument('-F', '--command_file', default='command_lines', help='file contains commands to run')
parser.add_argument('-W', '--waiting_time', default=30, type=int, help='time before checking if each thread have finished current job')
parser.add_argument('-L', '--logs_file', default='', help='save stdout and stderr to log file presented in this file, the line count must equal to commands, or it will be save to one log_all.txt under current directory')

args = parser.parse_args()


command_file_location=args.command_file
thread_number=args.threads
waiting_time_length=args.waiting_time
log_file_location=args.logs_file

cmd_list=list()
cmd_file=open(command_file_location,'r')
for lines in cmd_file:
	cmd_list.append(lines.split('\n')[0].split('\r')[0]+'\n')

if log_file_location!='':
	log_list=list()
	log_file=open(log_file_location,'r')
	for lines in log_file:
		log_list.append(lines.split('\n')[0].split('\r')[0])
	if len(cmd_list)!=len(log_list):
		print('log file number not equal to cmd number, saving logs to log_all.txt')
		log_list=list()
		for cmd in cmd_list:
			log_list.append('log_all.txt')

#create N threads
threads_list=list()
cmd_index=0
#Need_for_cycling=True#if there is not less commands than threads, then there is no need for cycling
logfiles=list()
for i in range(0,thread_number):
	if cmd_index>=len(cmd_list):
		#Need_for_cycling=False
		break
	new_thread=list()
	new_thread.append(cmd_list[cmd_index])	#cmd
	if log_file_location=='':
		new_thread.append(subprocess.Popen(cmd_list[cmd_index].split('\n')[0],shell=True))
	else:
		logfiles.append(open(log_list[cmd_index].split('\n')[0],'a'))
		new_thread.append(subprocess.Popen(cmd_list[cmd_index].split('\n')[0],shell=True,stdout=logfiles[cmd_index],stderr=logfiles[cmd_index]))
		new_thread.append(cmd_index)
	print(cmd_list[cmd_index])
	new_thread.append('going')#status
	threads_list.append(new_thread)
	cmd_index+=1
	
effective_thread_number=len(threads_list)

if log_file_location=='':
	status_index=2
else:
	status_index=3


strwrite_summary=''
stoped_num=0
while True:
	time.sleep(waiting_time_length)
	#wait for a moment
	for i in range(0,effective_thread_number):
		if threads_list[i][1].poll()!=None and threads_list[i][status_index]=='going':
			#finish current task
			if threads_list[i][1].poll()!=0:
				#error, should output message
				strwrite_summary+=str(threads_list[i][1].poll())+'\t'+threads_list[i][0]+'\n'
				#write error to summary
			threads_list[i][1].wait()
			#collected resource
			if log_file_location!='':
				logfiles[threads_list[i][2]].flush()
				logfiles[threads_list[i][2]].close()
			#flush log 
			if cmd_index<len(cmd_list):
				#still commands left
				threads_list[i][0]=cmd_list[cmd_index]
				if log_file_location=='':
					threads_list[i][1]=subprocess.Popen(cmd_list[cmd_index].split('\n')[0],shell=True)
				else:
					logfiles.append(open(log_list[cmd_index].split('\n')[0],'a'))
					threads_list[i][1]=subprocess.Popen(cmd_list[cmd_index].split('\n')[0],shell=True,stdout=logfiles[cmd_index],stderr=logfiles[cmd_index])
					threads_list[i][2]=cmd_index
				print(cmd_list[cmd_index])
				#new thread
				cmd_index+=1
			else:
				threads_list[i][status_index]='stop'
				print('thread '+str(i)+' stop')
				stoped_num+=1
				print(stoped_num)
			if stoped_num>=effective_thread_number:
				break
	if stoped_num>=effective_thread_number:
		break
file_summary=open('run_summary.txt','a')
file_summary.write(strwrite_summary)






#done





