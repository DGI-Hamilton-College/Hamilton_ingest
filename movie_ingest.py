'''
Created on Sep 19, 2011
This file handles the batch ingest of the civil war collection for hamilton
@author: William Panting


etree.tostring(tree)


'''
import logging, sys, os, ConfigParser, time#, shutil
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from islandoraUtils import xmlib
etree = xmlib.import_etree()


'''
Helper functions
'''

def handle_clip_mods(clip_mods_parser):
    '''
    This function will handle the creation of clip objects
    @param clip_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    print(clip_mods_parser)
    
def handle_misc_mods(misc_mods_parser):
    '''
    This function will handle the creation of clip objects
    @param misc_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    print(misc_mods_parser)
    
def handle_still_mods(still_mods_parser):
    '''
    This function will handle the creation of clip objects
    @param still_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    print(still_mods_parser)
    
def handle_transcript_mods(transcript_mods_parser):
    '''
    This function will handle the creation of clip objects
    @param transcript_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    print(transcript_mods_parser)
    
    
def handle_mods_file(mods_file_path):
    '''
    This function will determine which function to send the MODS data to for handling
    It will create an etree parser object and
    and then send it to the function which will handle the ingest
    @param modsFilePath
      The path to the mods file that describes what to ingest.
    @return boolean
      True on success, false if something was wrong
    @author William Panting
    '''
    print(modsFilePath)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        source_directory = sys.argv[1]
    else:
        print('Please verify source directory.')
        sys.exit(-1)
    

            
    '''
    setup
    '''
        
        
    #configure logging
    log_directory=os.path.join(source_directory,'logs')
    if not os.path.isdir(log_directory):
        os.mkdir(log_directory)
    logFile=os.path.join(log_directory,'Hamilton_batch_movie_ingest' + time.strftime('%y_%m_%d') + '.log')
    logging.basicConfig(filename=logFile, level=logging.DEBUG)

    #get config
    config = ConfigParser.ConfigParser()
    #config.read(os.path.join(source_directory,'TESTcfg'))
    config.read(os.path.join(source_directory,'TEST.cfg'))
    solrUrl=config.get('Solr','url')
    fedoraUrl=config.get('Fedora','url')
    fedoraUserName=config.get('Fedora', 'username')
    fedoraPassword=config.get('Fedora','password')
            
    #get fedora connection
    connection = Connection(fedoraUrl,
                    username=fedoraUserName,
                     password=fedoraPassword)
    try:
        fedora=FedoraClient(connection)
    except FedoraConnectionException:
        logging.error('Error connecting to fedora, exiting'+'\n')
        sys.exit()
    
    
    #setup the directories
    mods_directory = os.path.join(source_directory, 'mods')
    if not os.path.isdir(mods_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
    
    data_directory = os.path.join(source_directory, 'data')
    if not os.path.isdir(data_directory):
        logging.error('Data directory invalid \n')
        sys.exit()
        
    #prep data structures (files)
    mods_files = os.listdir(mods_directory)
    data_files = os.listdir(data_directory)
    
    
    '''
    do ingest
    '''
    #create a movie and benshi object because parent pids must be known
    #@todo: change the labels to be more meaningful later in the script
    mods_file = mods_files[0]
    
    movie_name = mods_file[:mods_file.find('-')]
    movie_pid = fedora.getNextPID(u'hamilton')
    benshi_pid = fedora.getNextPID(u'hamilton')
    
    movie_label=unicode(movie_name)
    movie_object = fedora.createObject(movie_pid, label = movie_label)
    
    benshi_label=unicode(movie_name + ' Benshi')
    benshi_object = fedora.createObject(benshi_pid, label = benshi_label)
    
    print('starter objects')
    
    #loop through the mods folder and ingest the clips because parent pids mst be known
    mods_files_copy = list(mods_files)
    for mods_file in mods_files_copy:
        #proceed if mods file is for a clip
        if '-cp' in mods_file:
            #ingest
            handle_mods_file(mods_file)
            #remove the file from mods_files list
            mods_files.remove(mods_file)
    print('clips')
    
    #loop through the rest of the files and ingest the data
    for mods_file in mods_files:
        handle_mods_file(mods_file)
        
sys.exit()