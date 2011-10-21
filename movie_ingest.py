'''
Created on Oct 17, 2011
This file handles the batch ingest of the civil war collection for hamilton
@author: William Panting
'''

import logging, sys, os, ConfigParser, time#, shutil
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from lxml import etree

'''
Helper functions
@todo: add datastreams 
@todo: check for missing files
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
    
    clip_label=unicode(movie_name + '_' + mods_file_name[mods_file_name.find('-')+1:mods_file_name.rfind('.')])
    clip_object = fedora.createObject(clip_pid, label = clip_label)
    clip_object_RELS_EXT=fedora_relationships.rels_ext(clip_object,[hamilton_rdf_name_space, fedora_model_namespace])

    global clips_to_pids
    clips_to_pids[mods_file_name] = clip_pid
    
    #this section handles the diferent types of clips (subs or not)
    if not '-sub' in mods_file_name:
            #add relationships
        clip_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isClipOf'), movie_object)
        clip_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiClip')
        clip_object_RELS_EXT.update()
        return True
    else:
        #add relationships
        master_clip_file_name = mods_file_name.replace('-sub','')
        clip_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isSubOf'), clips_to_pids[master_clip_file_name])
        clip_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiClipSubbed')
        clip_object_RELS_EXT.update()
        return True
    return False
    
def handle_misc_mods(misc_mods_parser, mods_file_name):
    '''
    This function will handle the creation of clip objects
    @param misc_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''
    misc_type_list = misc_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'genre'][@type='local']")
    if misc_type_list:
        misc_type = misc_type_list[0].text
        print(misc_type)
        if misc_type == 'sound recording':#fix up benshi object
            print('benshi')
            
        elif misc_type == 'essay':
            misc_pid = fedora.getNextPID(name_space)
            misc_label = unicode(movie_name + '_' + misc_type)
            misc_object = fedora.createObject(misc_pid, label = misc_label)
            misc_object_RELS_EXT = fedora_relationships.rels_ext(misc_object,[hamilton_rdf_name_space, fedora_model_namespace])
        
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isEssayOf'), movie_pid)
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiEssay')
            misc_object_RELS_EXT.update()
            
        elif misc_type == 'presentation':
            misc_pid = fedora.getNextPID(name_space)
            misc_label = unicode(movie_name + '_' + misc_type)
            misc_object = fedora.createObject(misc_pid, label = misc_label)
            misc_object_RELS_EXT = fedora_relationships.rels_ext(misc_object,[hamilton_rdf_name_space, fedora_model_namespace])
        
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isPresentationOf'), movie_pid)
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiPresentation')
            misc_object_RELS_EXT.update()
            
        elif misc_type == 'Motion Picture':#fix up movie object
            print('movie')
            
        elif misc_type == 'biography':
            misc_pid = fedora.getNextPID(name_space)
            misc_label = unicode(movie_name + '_Narrator')
            misc_object = fedora.createObject(misc_pid, label = misc_label)
            misc_object_RELS_EXT = fedora_relationships.rels_ext(misc_object,[hamilton_rdf_name_space, fedora_model_namespace])
        
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isNarratorOf'), benshi_pid)
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiNarrator')
            misc_object_RELS_EXT.update()

        else:
            return False
        
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
        still_pid = fedora.getNextPID(name_space)
        
        still_label = unicode(movie_name + '_' + mods_file_name[mods_file_name.find('-')+1:mods_file_name.rfind('.')])
        still_object = fedora.createObject(still_pid, label = still_label)
        still_object_RELS_EXT = fedora_relationships.rels_ext(still_object,[hamilton_rdf_name_space, fedora_model_namespace])
        
        
        
        #add relationships
        still_clip_element_list = still_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Video clip']")
        if still_clip_element_list:
            still_clip_file_name = still_clip_element_list[0].text
            still_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isStillOf'), clips_to_pids[still_clip_file_name])
        else:
            still_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isStillOf'), movie_pid)
        
        still_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiStill')
        still_object_RELS_EXT.update()
        
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
    
    transcript_pid = fedora.getNextPID(name_space)
    transcript_label = unicode(movie_name + '_' + mods_file_name[mods_file_name.find('-tr-') + 4:mods_file_name.rfind('.')])
    transcript_object = fedora.createObject(transcript_pid, label = transcript_label)
    transcript_object_RELS_EXT = fedora_relationships.rels_ext(transcript_object,[hamilton_rdf_name_space, fedora_model_namespace])
        
    transcript_element_list = transcript_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Document']")
    if transcript_element_list:
        transcript_path = transcript_element_list[0].text
        transcript_path = os.path.normpath(os.path.join(mods_directory, transcript_path))
        
        #handle is transcript of
        transcript_clip_element_list = transcript_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Video clip']")
        if transcript_clip_element_list:
            transcript_clip_file_name = transcript_clip_element_list[0].text
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isTranscriptOf'), clips_to_pids[transcript_clip_file_name])
        else:
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isTranscriptOf'), movie_name)
            
        #handle the 3 different transcript types
        if '-jpneng' in mods_file_name:
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':EnglishJapaneseTranscript')
        elif '-jpn' in mods_file_name:
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':JapaneseTranscript')
        elif '-eng' in mods_file_name:
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':EnglishTranscript')
        else:
            return False
    
        transcript_object_RELS_EXT.update()
        
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
    name_space = u'hamilton4'
        
    hamilton_rdf_name_space = fedora_relationships.rels_namespace('hamilton', 'http://hamilton.org/ontology#')
    fedora_model_namespace = fedora_relationships.rels_namespace('fedora-model','info:fedora/fedora-system:def/model#')
    
    #configure logging
    log_directory = os.path.join(source_directory,'logs')
    if not os.path.isdir(log_directory):
        os.mkdir(log_directory)
    logFile=os.path.join(log_directory,'Hamilton_batch_movie_ingest' + time.strftime('%y_%m_%d') + '.log')
    logging.basicConfig(filename = logFile, level=logging.DEBUG)

    #get config
    config = ConfigParser.ConfigParser()
    #config.read(os.path.join(source_directory,'HAMILTON.cfg'))
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
            collection_object = fedora.createObject(collection_pid, label = collection_label)
            #add relationships
            collection_object_RELS_EXT=fedora_relationships.rels_ext(collection_object,fedora_model_namespace)
            collection_object_RELS_EXT.addRelationship('isMemberOf','islandora:top')
            collection_object_RELS_EXT.update()
    #put in the benshi Islandora:BenshiMovie content model
    try:
        model_pid = u'islandora:BenshiMovie'
        fedora.getObject(model_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info('islandora:BenshiMovie missing, creating object.\n')
            model_object = fedora.createObject(model_pid, label = u'BenshiMovieCModel')
            #add relationships
            model_object_RELS_EXT=fedora_relationships.rels_ext(model_object,fedora_model_namespace)
            model_object_RELS_EXT.addRelationship('isMemberOf','islandora:top')
            model_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:collectionCModel')
            model_object_RELS_EXT.update()
    
    
    movie_name = mods_file_name[:mods_file_name.find('-')]
    movie_pid = fedora.getNextPID(name_space)
    benshi_pid = fedora.getNextPID(name_space)
    
    movie_label=unicode(movie_name)
    movie_object = fedora.createObject(movie_pid, label = movie_label)
    #add relationships
    movie_object_RELS_EXT=fedora_relationships.rels_ext(movie_object,fedora_model_namespace)
    movie_object_RELS_EXT.addRelationship('isMemberOf',collection_pid)
    movie_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), model_pid)
    movie_object_RELS_EXT.update()
    
    benshi_label=unicode(movie_name + ' Benshi')
    benshi_object = fedora.createObject(benshi_pid, label = benshi_label)
    #add relationships
    benshi_object_RELS_EXT=fedora_relationships.rels_ext(benshi_object,[hamilton_rdf_name_space, fedora_model_namespace])
    benshi_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton', 'isBenshiOf'), movie_object)
    benshi_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiRecording')
    benshi_object_RELS_EXT.update()
    
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