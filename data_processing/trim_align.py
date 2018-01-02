# Built in packages:
# datatime helps keep track of when the function is called
# getpass propts for password without echoing
# logging deals with logging 
import logging, datetime, socket, getpass, re, sys
import paramiko # implementation of SSHv2 protocol (http://www.paramiko.org/)

class TrimAndAlign:
    def __init__(self, log_file="Trim and Align log.log"):
        """
            Creates an ssh connection, log file and connection to server
        """
        # software locations on server
        self.btrim_dir = "/opt/Btrim"
        self.bowtie_dir = "/opt/bowtie-1.2.1.1"
        self.bowtie2_dir = "/opt/bowtie2-2.3.3"
        self.samtools_dir = "/opt/samtools-0.1.19/bin"
        self.tophat_dir = "/opt/tophat-2.1.1"

        # server address
        self.server_address = "p-bioresearch.coh.org"

        # server directory to work in
        self.server_directory = "/data/jkurata/"

        self.ssh = paramiko.SSHClient()
        self.createLog(log_file)
        self.connectToServer()

    def __del__(self):
        """
            Closes connection to server and log when instance is destroyed
        """
        self.closeConnection()
        self.closeLog()
        
    def createLog(self, log_file):
        """
            Sets up log file
        """
        self.logger = logging.getLogger('trimAlign')
        fhandler = logging.FileHandler(filename=log_file, mode="a")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fhandler.setFormatter(formatter)
        self.logger.addHandler(fhandler)
        self.logger.setLevel(logging.INFO)

    def closeLog(self):
        # Close log file
        handlers = self.logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)
        logging.shutdown()
            
    def connectToServer(self):
        """
        Makes the actual connection to the server and prompts for 
        username and password. 
        Also handles errors in connecting to the server
        """
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # prompts the user for username and password
        usrName = raw_input("Server user name: ")
        psswrd = getpass.getpass("Server password: ")
        try:
            self.ssh.connect(self.server_address, username=usrName,
                             password=psswrd)
            self.logger.info("Successfully connected to server")
        except paramiko.AuthenticationException:
            self.logger.error("Could Not Connect to Server: Authentication Failed", exc_info=True)
            sys.exit("Authentication failed")
        except paramiko.SSHException:
            self.logger.error("Could Not Connect to Server: SHH Exception", exc_info=True)
            sys.exit("Connection failure: SHH Exception")
        except socket.error:
            self.logger.error("Could Not Connect to Server: Socket Error", exc_info=True)
            sys.exit("Connection failure: Socket Error")
        except:
            self.logger.error("Unexpected Error: ", exc_info=True)
            sys.exit("Unknown Connection Error")

    def closeConnection(self):
        """
        Closes the ssh connection to the server
        """
        self.ssh.close()
        self.logger.info("Closed connection to server")
        
    def fileToServer(self, curLocation, fname, ext=''):
        """
        Will move file at curLocation to the server
        On the server, file with be at server_directory+fname.ext
        """
        self.logger.info("Moving file {} to server".format(curLocation))
        sftp = self.ssh.open_sftp()
        placeToPut = self.server_directory+fname+ext
        sftp.put(curLocation, placeToPut)
        self.logger.info("File {} sucessfully moved to {} on server".format(curLocation, placeToPut))
    
    def fileFromServer(self, locLocation, fname, ext=''):
        """
        Will move file from server_directory+fname.ext
        to locLocation on local computer where it will have the name fname.ext
        """
        placeToPut = locLocation + fname +ext
        placeToGet = self.server_directory+fname+ext
        
        self.logger.info("Copying file {} from server to {}".format(placeToGet, placeToPut))
        sftp = self.ssh.open_sftp()
        # Can also use callback=func(int,int) to get # of bytes transferred and total bytes
        sftp.get(placeToGet, placeToPut)
        self.logger.info("The file has successfully been moved")
        
    def trim(self, trimName, sampName, param="-l 16"):
        """
            Submit the name of a file containing the sequences to be removed
            and the name of the file with the reads
            The -l 16 indicates reads longer than 16 bp after trimming should be kept. Default is 25 bp
        """
        tFile = trimName
        sFile = sampName + '.fastq'
        oFile = sampName + '_trimmed.fastq'

        command_str = "export PATH=$PATH:{}; cd {}; btrim64 {} -t {} -p {} -o {}".format(self.btrim_dir, self.server_directory, param, sFile, tFile, oFile)
        
        self.logger.info("Start trimming using command: {}".format(command_str))
        
        stdin, stdout, stderr = self.ssh.exec_command(command_str)
        for line in stdout:
            self.logger.info(line)
        for line in stderr:
            self.logger.error(line)
        
        self.logger.info("Finished trimming sample {}".format(sampName))
        
    def makeIndex(self, fileList, indexName):
        """
            Makes a bowtie2 index with name indexName from a list of files containing the DNA sequences
        """
        # Concatenate the list of files together
        files = ",".join(fileList)

        command_str = "export PATH=$PATH:{};cd {}; bowtie2-build {} {}".format(self.bowtie2_dir, self.server_directory, files, indexName)
        
        self.logger.info("Start making index using command: {}".format(command_str))
        
        stdin, stdout, stderr = self.ssh.exec_command(command_str)
        for line in stdout:
            self.logger.info(line)
            
        self.logger.info("Finished making bowtie2 index {}".format(indexName))

    def makeIndex_bowtie(self, fileList, indexName):
        """
            Makes a bowtie index with name indexName from a list of files containing the DNA sequences
        """
        # Concatenate the list of files together
        files = ",".join(fileList)

        command_str = "export PATH=$PATH:{};cd {}; bowtie-build {} {}".format(self.bowtie_dir, self.server_directory, files, indexName)
        
        self.logger.info("Start making index using command: {}".format(command_str))
        
        stdin, stdout, stderr = self.ssh.exec_command(command_str)
        for line in stdout:
            self.logger.info(line)
            
        self.logger.info("Finished making bowtie index {}".format(indexName))
        
    def align(self, sampName, indexName, processors=1, options="-L 6 -i S,1,0.7"):
        """
            Submit the name of the file with the reads to be aligned and the index to align them to
        """
        sFile = sampName+'.fastq'
        oFile = sampName+'_aligned.sam'
        # The -L tells length of seed to use
        # The -i denotes the equation to use to calculate the intervals between seed substrings
        command_str = 'export PATH=$PATH:{}; cd {}; bowtie2 {} -p {} -x {} \
        -U {} -S {}'.format(self.bowtie2_dir, self.server_directory, options, processors, indexName, sFile, oFile)
        
        self.logger.info("Start alignment using command: {}".format(command_str))
        
        stdin, stdout, stderr = self.ssh.exec_command(command_str)
        # For some reason, bowtie2 prints the alignment output as errors
        for line in stderr:
            self.logger.info(line)

    def align_bowtie(self, sampName, indexName, options="-v 1 --best -S"):
        """
            Align reads using bowtie, not bowtie2
            Defaults to allowing only 1 mismatch between read and reference (-v 1) and to determining the match with the lowest number of mismatches
        """
        sFile = sampName+'.fastq'
        oFile = sampName+'_bowtie-aligned.sam'
        
        command_str = "export PATH=$PATH:{}; cd {}; bowtie {} {} -q {} {}".format(self.bowtie_dir, 
        self.server_directory, options, indexName, sFile, oFile)
        
        self.logger.info("Start alignment using command: {}".format(command_str))
        
        stdin, stdout, stderr = self.ssh.exec_command(command_str)
        # For some reason, bowtie prints the alignment output as errors
        for line in stderr:
            self.logger.info(line)

    def align_tophat(self, sampName, indexName, options="-G Homo_sapiens.GRCh38.88_wChr.gtf", processors=1):
        """
            Align reads using tophat, which is splicing aware
            Use for RNA-seq data
        """
        sFile = sampName+'.fastq'
        oDir = "./"+sampName
        
        # tophat requires bowtie2 and samtools be in PATH
        # need to use beta version of bowtie2 due to how versioning is checked
        command_str = "export PATH=$PATH:{}:{}:{}; cd {}; tophat {} -p {} -o {} {} {}".format(self.bowtie2_dir, 
        self.samtools_dir, self.tophat_dir, self.server_directory, options, processors, oDir, indexName, sFile)
        
        self.logger.info("Start alignment using command: {}".format(command_str))
        
        stdin, stdout, stderr = self.ssh.exec_command(command_str)
        # For some reason, bowtie prints the alignment output as errors
        for line in stderr:
            self.logger.info(line)
        for line in stdout:
            self.logger.info(line)
    
    def cleanUp(self, sampName):
        self.logger.info("Cleaning up...")
        match = re.match('^(.*?)_(trimmed|aligned|bowtie-aligned)', sampName)
        try:
            name = match.group(1)
            command_str = "cd {}; rm {}*.fastq; rm {}*.sam".format(self.server_directory, name, name)
            self.logger.info("Deleting files using command: {}".format(command_str))
            stdin, stdout, stderr = self.ssh.exec_command(command_str)
            for line in stdout:
                self.logger.info(line)
            for line in stderr:
                self.logger.error(line)
        except AttributeError:
            self.logger.error('The files for sample {} could not be cleaned up'.format(sampName))
            
