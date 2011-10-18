'''
Created on Oct 17, 2011
This file handles the batch ingest of the civil war collection for hamilton
@author: William Panting
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

def handle_clip_mods(clip_mods_parser, mods_file_path):
    '''
    This function will handle the creation of clip objects
    @param clip_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    high_resolution_element_list = clip_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='High Quality Video']")
    if high_resolution_element_list:
        high_resolution_mov_path = high_resolution_element_list[0].text
    
    low_resolution_element_list = clip_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Web Quality Video']")
    if low_resolution_element_list:
        low_resolution_mov_path = low_resolution_element_list[0].text
    
    clip_pid = fedora.getNextPID(name_space)
    
    high_resolution_mov_path = os.path.join(os.path.dirname(mods_file_path), high_resolution_mov_path)
    low_resolution_mov_path = os.path.join(os.path.dirname(mods_file_path), low_resolution_mov_path)
    
    #this section handles the diferent types of clips (subs or not)
    if not '-sub' in os.path.basename(mods_file_path):
        global clips_to_pids
        clips_to_pids[mods_file] = clip_pid
        print('clip')
    else:
        print('clip with subs')
        
    return True
    
def handle_misc_mods(misc_mods_parser, mods_file):
    '''
    This function will handle the creation of clip objects
    @param misc_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    
    print('misc')
    return True

def handle_still_mods(still_mods_parser, mods_file):
    '''
    This function will handle the creation of clip objects
    @param still_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    
    print('still')
    return True

def handle_transcript_mods(transcript_mods_parser, mods_file):
    '''
    This function will handle the creation of clip objects
    @param transcript_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    
    print('transcript')
    return True

    
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
    mods_tree = etree.parse(mods_file_path)
    if '-cp' in mods_file_path:
        if handle_clip_mods(mods_tree, mods_file_path) is False:
            return False
    if '-mis' in mods_file_path:
        if handle_misc_mods(mods_tree, mods_file_path) is False:
            return False
    if '-st' in mods_file_path:
        if handle_still_mods(mods_tree, mods_file_path) is False:
            return False
    if '-tr-' in mods_file_path:
        if handle_transcript_mods(mods_tree, mods_file_path) is False:
            return False    
    return True

if __name__ == '__main__':
    if len(sys.argv) == 2:
        source_directory = sys.argv[1]
    else:
        print('Please verify source directory.')
        sys.exit(-1)
    

            
    '''
    setup
    '''
    name_space = u'hamilton1'
        
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
    
    #restrict mods_files to .xml
    mods_files_copy = list(mods_files)
    for mods_file in mods_files_copy:
        if not mods_file.endswith('.xml'):
            mods_files.remove(mods_file)
    
    #this mapping is to link pids to clip files to make building relationships easier
    clips_to_pids = dict({'1':'1', '1':'1'})
    
    '''
    do ingest
    '''
    #create a movie and benshi object because parent pids must be known
    #@todo: change the labels to be more meaningful later in the script
    mods_file = mods_files[0]
    
    movie_name = mods_file[:mods_file.find('-')]
    movie_pid = fedora.getNextPID(name_space)
    benshi_pid = fedora.getNextPID(name_space)
    
    movie_label=unicode(movie_name)
    movie_object = fedora.createObject(movie_pid, label = movie_label)
    
    benshi_label=unicode(movie_name + ' Benshi')
    benshi_object = fedora.createObject(benshi_pid, label = benshi_label)
    
    print('starter objects')
    
    #loop through the mods folder and ingest the clips because parent pids mst be known
    mods_files_copy = list(mods_files)
    for mods_file in mods_files_copy:
        #proceed if mods file is for a clip but not one with subs
        if '-cp' in mods_file and not '-sub' in mods_file:
            mods_file_full_path = os.path.join(mods_directory, mods_file)
            #ingest
            if handle_mods_file(mods_file_full_path) is False:
                logging.error('Error handling mods_file: ' + mods_file_full_path + ' \n')
                sys.exit()
            #remove the file from mods_files list
            mods_files.remove(mods_file)
    print('clips')
    
    #loop through the rest of the files and ingest the data
    for mods_file in mods_files:
        mods_file_full_path = os.path.join(mods_directory, mods_file)
        if handle_mods_file(mods_file_full_path) is False:
            logging.error('Error handling mods_file: ' + mods_file_full_path + ' \n')
            sys.exit()
        
sys.exit()