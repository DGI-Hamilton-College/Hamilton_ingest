'''
Created on Oct 17, 2011
This file handles the batch ingest of the civil war collection for hamilton
@author: William Panting
'''

import logging, sys, os, ConfigParser, time
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from lxml import etree

'''
Helper functions
'''

def add_MODS_datastream(fedora_object, mods_file_path):
    '''
    This is a helper function for adding a mods datastream to an object... this was hapening for every object
    @param fedora_object
      fcrepo object to add the mods to
    @param mods_file_path
      the path to the mods file to upload as a datastream to fedora_objet
    '''
    mods_file_handle = open(mods_file_path)
    mods_contents = mods_file_handle.read()
    mods_file_handle.close()
    fedora_object_pid = fedora_object.pid
    try:
        fedora_object.addDataStream(u'MODS', unicode(mods_contents), label=u'MODS',
        mimeType = u'text/xml', controlGroup = u'X',
        logMessage = u'Added basic mods meta data.')
        logging.info('Added MODS datastream to:' + fedora_object_pid)
    except FedoraConnectionException:
        logging.error('Error in adding MODS datastream to:' + fedora_object_pid + '\n')
                
def get_file_path_from_xpath(lxml_parser, xpath_string):
    '''
    This function will return a file path from the mods metadata
    @param lxml_parser
      The lxml parser to run the xpath on
    @param xpath_string
      the xpath to the string with the mods metadata
    '''
    path_list = lxml_parser.xpath(xpath_string)
    if path_list:
        if 'currently unavailable' not in path_list[0].text:
            path = path_list[0].text
            return os.path.normpath(os.path.join(mods_directory, path))
    return False
                
def handle_clip_mods(clip_mods_parser, mods_file_name):
    '''
    This function will handle the creation of clip objects
    @param clip_mods_parser
      The etree xml parser to get ingest data from 
    @return boolean
      True on success, false if something was wrong
    '''    
    clip_pid = fedora.getNextPID(name_space)
    
    high_resolution_mov_path = get_file_path_from_xpath(clip_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='High Quality Video']")
    low_resolution_mov_path = get_file_path_from_xpath(clip_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Web Quality Video']")
    

    clip_number =  mods_file_name[mods_file_name.find('-cp') + 3:mods_file_name.rfind('.')]
    clip_number = mods_file_name.replace('-sub','')
    
    clip_label=unicode(movie_name + '_' + mods_file_name[mods_file_name.find('-')+1:mods_file_name.rfind('.')])
    clip_object = fedora.createObject(clip_pid, label = clip_label)
    #datastreams
    add_MODS_datastream(clip_object, mods_file_path)
    if high_resolution_mov_path:
        hires_file_handle = open(high_resolution_mov_path, 'rb')
        try:
            clip_object.addDataStream(u'HIGHRES', u'aTmpStr', label = u'HIGHRES',
            mimeType = u'video/quicktime', controlGroup = u'M',
            logMessage = u'Added HIGHRES datastream.')
            datastream = clip_object['HIGHRES']
            datastream.setContent(hires_file_handle)
            logging.info('Added HIGHRES datastream to:' + clip_pid)
        except FedoraConnectionException:
            logging.error('Error in adding HIGHRES datastream to:' + clip_pid + '\n')
        hires_file_handle.close()
    
    if low_resolution_mov_path:
        lowres_file_handle = open(low_resolution_mov_path, 'rb')
        try:
            clip_object.addDataStream(u'LOWRES', u'aTmpStr', label=u'LOWRES',
            mimeType = u'video/quicktime', controlGroup = u'M',
            logMessage = u'Added LOWRES datastream.')
            datastream = clip_object['LOWRES']
            datastream.setContent(lowres_file_handle)
            logging.info('Added LOWRES datastream to:' + clip_pid)
        except FedoraConnectionException:
            logging.error('Error in adding LOWRES datastream to:' + clip_pid + '\n')
        lowres_file_handle.close()
    
    #relationships
    clip_object_RELS_EXT = fedora_relationships.rels_ext(clip_object,[hamilton_rdf_name_space, fedora_model_namespace])
    clip_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isClipOf'), movie_pid)
    clip_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isClipNumber'), fedora_relationships.rels_object(str(clip_number), fedora_relationships.rels_object.LITERAL))
    
    global clips_to_pids
    clips_to_pids[mods_file_name] = clip_pid
    
    #this section handles the diferent types of clips (subs or not)
    if not '-sub' in mods_file_name:
        #add relationships
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
            #datastreams
            add_MODS_datastream(benshi_object, mods_file_path)
            audio_file_path = get_file_path_from_xpath(misc_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Audio']")
            
            if audio_file_path:
                audio_file_handle = open(audio_file_path, 'rb')
                try:
                    benshi_object.addDataStream(u'MP3', u'aTmpStr', label=u'MP3',
                    mimeType = u'audio/mpeg', controlGroup = u'M',
                    logMessage = u'Added MP3 datastream.')
                    datastream = benshi_object['MP3']
                    datastream.setContent(audio_file_handle)
                    logging.info('Added MP3 datastream to:' + benshi_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding MP3 datastream to:' + benshi_pid + '\n')
                audio_file_handle.close()
                print(audio_file_path)
            
        elif misc_type == 'essay':
            print('essay')
            misc_pid = fedora.getNextPID(name_space)
            misc_label = unicode(movie_name + '_' + misc_type)
            misc_object = fedora.createObject(misc_pid, label = misc_label)
            misc_object_RELS_EXT = fedora_relationships.rels_ext(misc_object,[hamilton_rdf_name_space, fedora_model_namespace])
            #datastreams
            add_MODS_datastream(misc_object, mods_file_path)
            
            essay_file_path = get_file_path_from_xpath(misc_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Article']")
            if essay_file_path:
                essay_file_handle = open(essay_file_path, 'rb')
                try:
                    misc_object.addDataStream(u'DOCX', u'aTmpStr', label=u'DOCX',
                    mimeType = u'application/vnd.openxmlformats-officedocument.wordprocessingml.document', controlGroup = u'M',
                    logMessage = u'Added DOCX datastream.')
                    datastream = misc_object['DOCX']
                    datastream.setContent(essay_file_handle)
                    logging.info('Added DOCX datastream to:' + misc_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding DOCX datastream to:' + misc_pid + '\n')
                essay_file_handle.close()
            
            #relationships
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isEssayOf'), movie_pid)
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiEssay')
            misc_object_RELS_EXT.update()
            
        elif misc_type == 'presentation':
            misc_pid = fedora.getNextPID(name_space)
            misc_label = unicode(movie_name + '_' + misc_type)
            misc_object = fedora.createObject(misc_pid, label = misc_label)
            misc_object_RELS_EXT = fedora_relationships.rels_ext(misc_object,[hamilton_rdf_name_space, fedora_model_namespace])
            #datastreams
            add_MODS_datastream(misc_object, mods_file_path)
            
            presentation_file_path = get_file_path_from_xpath(misc_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Presentation']")
            if presentation_file_path:
                presentation_file_handle = open(presentation_file_path, 'rb')
                try:
                    misc_object.addDataStream(u'PPTX', u'aTmpStr', label=u'PPTX',
                    mimeType = u'application/vnd.openxmlformats-officedocument.presentationml.presentation', controlGroup = u'M',
                    logMessage = u'Added PPTX datastream.')
                    datastream = misc_object['PPTX']
                    datastream.setContent(presentation_file_handle)
                    logging.info('Added PPTX datastream to:' + misc_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding PPTX datastream to:' + misc_pid + '\n')
                presentation_file_handle.close()
            
            #relationships
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isPresentationOf'), movie_pid)
            misc_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiPresentation')
            misc_object_RELS_EXT.update()
            
        #movie gets the opac redirect it's special
        elif misc_type == 'Motion Picture':#fix up movie object
            print('movie')
            #datastreams
            add_MODS_datastream(movie_object, mods_file_path)
            
            opac_path = get_file_path_from_xpath(misc_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url']")
            if opac_path:
                try:
                    movie_object.addDataStream(u'OPAC', u'aTmpStr', label = u'OPAC',
                    mimeType = u'text/html', controlGroup = u'R',
                    location = unicode(opac_path),
                    logMessage = u'Added OPAC datastream.')
                    logging.info('Added OPAC datastream to:' + movie_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding OPAC datastream to:' + movie_pid + '\n')
            
            #biography is special it has a docx and a pdf
            #can't use 'get_file_path_from_xpath' these are different then the rest, can change it or handle things here (handle things here, dev_speed)
        elif misc_type == 'biography':
            misc_pid = fedora.getNextPID(name_space)
            misc_label = unicode(movie_name + '_Narrator')
            misc_object = fedora.createObject(misc_pid, label = misc_label)
            misc_object_RELS_EXT = fedora_relationships.rels_ext(misc_object,[hamilton_rdf_name_space, fedora_model_namespace])
            
            #get the paths for the pdf/docx
            docx_file_path = False
            pdf_file_path = False
            path_list = misc_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Article']")
            if path_list:
                for path_element in path_list:
                    if 'currently unavailable' not in path_element:
                        if '.docx' in path_element.text:
                            docx_file_path = os.path.normpath(os.path.join(mods_directory, path_element.text))
                        elif 'pdf' in path_element.text:
                            pdf_file_path = os.path.normpath(os.path.join(mods_directory, path_element.text))
            
            
            #datastreams
            add_MODS_datastream(misc_object, mods_file_path) 
            
            if docx_file_path:
                docx_file_handle = open(docx_file_path, 'rb')
                try:
                    misc_object.addDataStream(u'DOCX', u'aTmpStr', label = u'DOCX',
                    mimeType = u'application/vnd.openxmlformats-officedocument.wordprocessingml.document', controlGroup = u'M',
                    logMessage = u'Added DOCX datastream.')
                    datastream = misc_object['DOCX']
                    datastream.setContent(docx_file_handle)
                    logging.info('Added DOCX datastream to:' + misc_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding DOCX datastream to:' + misc_pid + '\n')
                docx_file_handle.close()
            
            if pdf_file_path:
                pdf_file_handle = open(pdf_file_path, 'rb')
                try:
                    misc_object.addDataStream(u'PDF', u'aTmpStr', label = u'PDF',
                    mimeType = u'application/pdf', controlGroup = u'M',
                    logMessage = u'Added PDF datastream.')
                    datastream = misc_object['PDF']
                    datastream.setContent(pdf_file_handle)
                    logging.info('Added PDF datastream to:' + misc_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding PDF datastream to:' + misc_pid + '\n')
                pdf_file_handle.close()
            
            #relationships
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
    still_path = get_file_path_from_xpath(still_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Still Image']")
    
    still_pid = fedora.getNextPID(name_space)
    still_label = unicode(movie_name + '_' + mods_file_name[mods_file_name.find('-')+1:mods_file_name.rfind('.')])
    still_object = fedora.createObject(still_pid, label = still_label)
    still_object_RELS_EXT = fedora_relationships.rels_ext(still_object,[hamilton_rdf_name_space, fedora_model_namespace])
        
        #datastreams
    add_MODS_datastream(still_object, mods_file_path)
    if still_path:
        png_file_handle = open(still_path, 'rb')
        try:
            still_object.addDataStream(u'PNG', u'aTmpStr', label=u'PNG',
            mimeType = u'image/png', controlGroup = u'M',
            logMessage = u'Added PNG datastream.')
            datastream = still_object['PNG']
            datastream.setContent(png_file_handle)
            logging.info('Added PNG datastream to:' + still_pid)
        except FedoraConnectionException:
            logging.error('Error in adding PNG datastream to:' + still_pid + '\n')
        png_file_handle.close()
        
        #relationships
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
    transcript_path = get_file_path_from_xpath(transcript_mods_parser, "//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Document']")
    time_synced_transcript_path = transcript_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Document with time-sync encoding']")
    
    
    #datastreams
    add_MODS_datastream(transcript_object, mods_file_path)
        
    if time_synced_transcript_path:
        print(time_synced_transcript_path)
        time_synced_transcript_handle = open(time_synced_transcript_path, 'rb')
        try:
            transcript_object.addDataStream(u'TimeSyncedTranscript', u'aTmpStr', label=u'POPCORN',
            mimeType = u'application/xml', controlGroup = u'M',
            logMessage = u'Added TimeSyncedTranscript datastream.')
            datastream = transcript_object['TimeSyncedTranscript']
            datastream.setContent(time_synced_transcript_handle)
            logging.info('Added TimeSyncedTranscript datastream to:' + transcript_pid)
        except FedoraConnectionException:
            logging.error('Error in adding TimeSyncedTranscript datastream to:' + transcript_pid + '\n')
        time_synced_transcript_handle.close()
        
    if transcript_path:
        add_MODS_datastream(transcript_object, mods_file_path)
        pdf_file_handle = open(transcript_path, 'rb')
        try:
            transcript_object.addDataStream(u'PDF', u'aTmpStr', label=u'PDF',
            mimeType = u'application/pdf', controlGroup = u'M',
            logMessage = u'Added PDF datastream.')
            datastream = transcript_object['PDF']
            datastream.setContent(pdf_file_handle)
            logging.info('Added PDF datastream to:' + transcript_pid)
        except FedoraConnectionException:
            logging.error('Error in adding PDF datastream to:' + transcript_pid + '\n')
        pdf_file_handle.close()
    
    #relationships
        #handle is transcript of
        transcript_clip_element_list = transcript_mods_parser.xpath("//*[local-name() = 'mods']//*[local-name() = 'location']//*[local-name() = 'url'][@displayLabel='Video clip']")
        if transcript_clip_element_list:
            transcript_clip_file_name = transcript_clip_element_list[0].text
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isTranscriptOf'), clips_to_pids[transcript_clip_file_name])
        else:
            transcript_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton','isTranscriptOf'), movie_pid)
            
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
    name_space = u'hamilton6'
        
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
        logging.error('Error connecting to fedora, exiting \n')
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
        model_pid = u'islandora:benshiMovie'
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
    movie_object_RELS_EXT.addRelationship('isMemberOf', collection_pid)
    movie_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), model_pid)
    movie_object_RELS_EXT.update()
    
    benshi_label=unicode(movie_name + ' Benshi')
    benshi_object = fedora.createObject(benshi_pid, label = benshi_label)
    #add relationships
    benshi_object_RELS_EXT=fedora_relationships.rels_ext(benshi_object,[hamilton_rdf_name_space, fedora_model_namespace])
    benshi_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('hamilton', 'isBenshiOf'), movie_pid)
    benshi_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'), name_space + ':benshiRecording')
    benshi_object_RELS_EXT.update()
    
    print('starter objects ingested')
    
    #loop through the mods folder and ingest the clips because parent pids must be known
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