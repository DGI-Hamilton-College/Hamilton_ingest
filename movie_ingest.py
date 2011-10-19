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

def handle_clip_mods(clip_mods_parser, mods_file_name):
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
    
    high_resolution_mov_path = os.path.normpath(os.path.join(mods_directory, high_resolution_mov_path))
    low_resolution_mov_path = os.path.normpath(os.path.join(mods_directory, low_resolution_mov_path))
    
    
    #this section handles the diferent types of clips (subs or not)
    if not '-sub' in mods_file_name:
        global clips_to_pids
        clips_to_pids[mods_file_name] = clip_pid
        print('clip')
    else:
        print('clip with subs')
        
    return True
    
def handle_misc_mods(misc_mods_parser, mods_file_name):
    '''
    This function will handle the creation of clip objects
    @param misc_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    '''
    misc_element_list = misc_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Web Quality Video']")
    if miscelement_list:
        misc_path = misc_element_list[0].text
        misc_path = os.path.normpath(os.path.join(mods_directory, misc_path))
        print(misc_path)
    '''
    print('misc')
    return True

def handle_still_mods(still_mods_parser, mods_file_name):
    '''
    This function will handle the creation of clip objects
    @param still_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    
    still_element_list = still_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Still Image']")
    if still_element_list:
        still_path = still_element_list[0].text
        still_path = os.path.normpath(os.path.join(mods_directory, still_path))
        print(still_path)
        print('still')
        return True
    return False

def handle_transcript_mods(transcript_mods_parser, mods_file_name):
    '''
    This function will handle the creation of clip objects
    @param transcript_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    
    transcript_element_list = transcript_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Document']")
    if transcript_element_list:
        transcript_path = transcript_element_list[0].text
        transcript_path = os.path.normpath(os.path.join(mods_directory, transcript_path))
        print(transcript_path)
        print('transcript')
        return True
    return False

    
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
    mods_file_name = os.path.basename(mods_file_path)
    
    if '-cp' in mods_file_path:
        if handle_clip_mods(mods_tree, mods_file_name) is False:
            return False
    if '-mis' in mods_file_path:
        if handle_misc_mods(mods_tree, mods_file_name) is False:
            return False
    if '-st' in mods_file_path:
        if handle_still_mods(mods_tree, mods_file_name) is False:
            return False
    if '-tr-' in mods_file_path:
        if handle_transcript_mods(mods_tree, mods_file_name) is False:
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
    mods_file_names = os.listdir(mods_directory)
    data_files = os.listdir(data_directory)
    
    #restrict mods_file_names to .xml
    mods_file_names_copy = list(mods_file_names)
    for mods_file_name in mods_file_names_copy:
        if not mods_file_name.endswith('.xml'):
            mods_file_names.remove(mods_file_name)
    
    #this mapping is to link pids to clip files to make building relationships easier
    clips_to_pids = dict()
    
    '''
    do ingest
    '''
    #create a movie and benshi object because parent pids must be known
    #@todo: change the labels to be more meaningful later in the script
    mods_file_name = mods_file_names[0]
    
    #put in the JapaneseSilentFilmCollection collection object
    try:
        collection_label = u'JapaneseSilentFilmCollection'
        collection_pid = unicode(name_space + ':' + collection_label)
        fedora.getObject(collection_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info(name_space + ':JapaneseSilentFilmCollection missing, creating object.\n')
            '''
            collection_object = fedora.createObject(collection_pid, label = collection_label)
            '''
    #put in the benshi Islandora:BenshiMovie content model
    try:
        model_pid = u'islandora:BenshiMovie'
        fedora.getObject(model_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info('islandora:BenshiMovie missing, creating object.\n')
            '''
            model_object = fedora.createObject(model_pid, label = u'BenshiMovieCModel')
            '''
    
    
    movie_name = mods_file_name[:mods_file_name.find('-')]
    movie_pid = fedora.getNextPID(name_space)
    benshi_pid = fedora.getNextPID(name_space)
    
    movie_label=unicode(movie_name)
    movie_object = fedora.createObject(movie_pid, label = movie_label)
    
    benshi_label=unicode(movie_name + ' Benshi')
    benshi_object = fedora.createObject(benshi_pid, label = benshi_label)
    
    print('starter objects ingested')
    
    #loop through the mods folder and ingest the clips because parent pids mst be known
    mods_file_names_copy = list(mods_file_names)
    for mods_file_name in mods_file_names_copy:
        #proceed if mods file is for a clip but not one with subs
        if '-cp' in mods_file_name and not '-sub' in mods_file_name:
            mods_file_path = os.path.join(mods_directory, mods_file_name)
            #ingest
            if handle_mods_file(mods_file_path) is False:
                logging.error('Error handling mods_file_name: ' + mods_file_path + ' \n')
                sys.exit()
            #remove the file from mods_file_names list
            mods_file_names.remove(mods_file_name)
    print('Main Clips ingested')
    
    #loop through the rest of the files and ingest the data
    for mods_file_name in mods_file_names:
        mods_file_path = os.path.join(mods_directory, mods_file_name)
        if handle_mods_file(mods_file_path) is False:
            logging.error('Error handling mods_file_name: ' + mods_file_path + ' \n')
            sys.exit()
        
sys.exit()