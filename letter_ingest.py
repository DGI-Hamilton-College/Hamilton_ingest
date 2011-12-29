'''
Created on Sep 19, 2011
This file handles the batch ingest of the civil war collection for hamilton
@author: William Panting
@todo: create a collection object and make all letters have the isMemberOf relationship to them.
@todo: change the page pids to follow the examples in the release beta
'''
import logging, sys, os, ConfigParser, time, subprocess#, shutil
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships
from lxml import etree

if __name__ == '__main__':
    if len(sys.argv) == 2:
        source_directory = sys.argv[1]
    else:
        print('Please verify source directory.')
        sys.exit(-1)
    
    
    '''
    setup
    '''
    hamilton_rdf_name_space = fedora_relationships.rels_namespace('hamilton', 'http://hamilton.org/ontology#')
    fedora_model_namespace = fedora_relationships.rels_namespace('fedora-model','info:fedora/fedora-system:def/model#')
    
        
    #configure logging
    log_directory = os.path.join(source_directory,'logs')
    if not os.path.isdir(log_directory):
        os.mkdir(log_directory)
    logFile = os.path.join(log_directory,'Hamilton_batch_letter_ingest' + time.strftime('%y_%m_%d') + '.log')
    logging.basicConfig(filename=logFile, level=logging.DEBUG)

    #get config
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(source_directory,'HAMILTON.cfg'))
    #config.read(os.path.join(source_directory,'TEST.cfg'))
    solrUrl = config.get('Solr','url')
    fedoraUrl = config.get('Fedora','url')
    fedoraUserName = config.get('Fedora', 'username')
    fedoraPassword = config.get('Fedora','password')
            
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
    mods_directory = os.path.join(source_directory, 'mods-xml')
    if not os.path.isdir(mods_directory):
        logging.error('MODS directory invalid \n')
        sys.exit()
    
    tei_directory = os.path.join(source_directory, 'tei-xml')
    if not os.path.isdir(tei_directory):
        logging.error('TEI directory invalid \n')
        sys.exit()
    
    tei_page_directory = os.path.join(source_directory, 'tei-xml' , 'pages')
    if not os.path.isdir(tei_page_directory):
        logging.error('TEI pages directory invalid \n')
        sys.exit()
    
    jp2_directory = os.path.join(source_directory, 'images-jp2')
    if not os.path.isdir(jp2_directory):
        logging.error('JP2 directory invalid \n')
        sys.exit()
    
    pdf_directory = os.path.join(source_directory, 'images-pdf')
    if not os.path.isdir(pdf_directory):
        logging.error('PDF directory invalid \n')
        sys.exit()
    
    #prep data structures (files)
    mods_files = os.listdir(mods_directory)
    tei_files = os.listdir(tei_directory)
    tei_page_files = os.listdir(tei_page_directory)
    jp2_files = os.listdir(jp2_directory)
    pdf_files = os.listdir(pdf_directory)
    
    name_space = u'HamiltonCivilWar'
    #name_space = u'HamiltonCivilWar2'
    
    '''
    do ingest
    '''
        #put in the JapaneseSilentFilmCollection collection object
    try:
        collection_label = u'UnitedStatesCivilWarLetters'
        collection_pid = unicode(name_space + ':' + collection_label)
        collection_policy = u'<collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="" xsi:schemaLocation="http://www.islandora.ca">  <content_models>    <content_model dsid="ISLANDORACM" name="Book Content Model" namespace="islandora:1" pid="islandora:bookCModel"></content_model>  </content_models>  <search_terms></search_terms>  <staging_area></staging_area>  <relationship>isMemberOf</relationship></collection_policy>'
        fedora.getObject(collection_pid)
    except FedoraConnectionException, object_fetch_exception:
        if object_fetch_exception.httpcode in [404]:
            logging.info(name_space + ':UnitedStatesCivilWarLetters missing, creating object.\n')
            collection_object = fedora.createObject(collection_pid, label = collection_label)
            #collection_policy
            try:
                collection_object.addDataStream(u'COLLECTION_POLICY', collection_policy, label=u'COLLECTION_POLICY',
                mimeType=u'text/xml', controlGroup=u'X',
                logMessage=u'Added basic COLLECTION_POLICY data.')
                logging.info('Added COLLECTION_POLICY datastream to:' + collection_pid)
            except FedoraConnectionException:
                logging.error('Error in adding COLLECTION_POLICY datastream to:' + collection_pid + '\n')
            
            #add relationships
            collection_object_RELS_EXT = fedora_relationships.rels_ext(collection_object, fedora_model_namespace)
            collection_object_RELS_EXT.addRelationship('isMemberOf','islandora:root')
            collection_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:collectionCModel')
            collection_object_RELS_EXT.update()

    #loop through the mods folder
    for mods_file in mods_files:
        if mods_file.endswith('MODS.xml'):
            #get mods file contents
            mods_file_path = os.path.join(source_directory, 'mods-xml', mods_file)
            mods_file_handle = open(mods_file_path)
            mods_contents = mods_file_handle.read()
            
            #get book_label from mods title
            mods_tree = etree.parse(mods_file_path)
            book_label = mods_tree.xpath("*[local-name() = 'titleInfo']/*[local-name() = 'title']/text()")
            book_label = book_label[0]
            if len(book_label) > 255:
                book_label = book_label[0:250] + '...'
            print(book_label)
            book_label = unicode(book_label)
            
            #create a book object
            book_pid = fedora.getNextPID(name_space)
            book_object = fedora.createObject(book_pid, label = book_label)
            
            #add mods datastream
            mods_file_handle.close()
            try:
                book_object.addDataStream(u'MODS', unicode(mods_contents), label = u'MODS',
                mimeType = u'text/xml', controlGroup = u'X',
                logMessage = u'Added basic mods meta data.')
                logging.info('Added MODS datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MODS datastream to:' + book_pid + '\n')
            
            #add pdf ds
            
            book_name = mods_file[:mods_file.find('_')]
            pdf_file = book_name + '.pdf'
            pdf_file_path = os.path.join(source_directory, 'images-pdf', pdf_file)
            pdf_file_handle = open(pdf_file_path, 'rb')
            
            try:
                book_object.addDataStream(u'PDF', u'aTmpStr', label=u'PDF',
                mimeType = u'application/pdf', controlGroup = u'M',
                logMessage = u'Added PDF datastream.')
                datastream = book_object['PDF']
                datastream.setContent(pdf_file_handle)
                logging.info('Added PDF datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding PDF datastream to:' + book_pid + '\n')
            pdf_file_handle.close()
            
            #add tei ds
            tei_file = book_name + '_TEIP5.xml'
            tei_file_path = os.path.join(source_directory, 'tei-xml', tei_file)
            tei_file_handle = open(tei_file_path)
            tei_contents = tei_file_handle.read()
            tei_file_handle.close()
            try:
                book_object.addDataStream(u'TEI', unicode(tei_contents, encoding = 'UTF-8'), label=u'TEI',
                mimeType = u'application/tei+xml', controlGroup=u'M',
                logMessage = u'Added basic tei meta data.')
                logging.info('Added tei datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding tei datastream to:' + book_pid + '\n')
            
            #add relationships
            objRelsExt = fedora_relationships.rels_ext(book_object, fedora_model_namespace)
            objRelsExt.addRelationship('isMemberOf', collection_pid)
            objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:bookCModel')
            objRelsExt.update()
            
            #get the book page datastructures
            book_page_jp2_files = list()
            for jp2_file in jp2_files:
                if jp2_file[:jp2_file.find('-')] == book_name:
                    book_page_jp2_files.append(jp2_file)
                    
            book_page_tei_files = list()
            for tei_page_file in tei_page_files:
                if tei_page_file[:tei_page_file.find('_')] == book_name:
                    book_page_tei_files.append(tei_page_file)
            #loop through the jp2 files that are associated with the mods
            for jp2_file in book_page_jp2_files:
                #create an object for each
                page_name = jp2_file[jp2_file.find('-') + 1:jp2_file.find('.')]
                #page_pid = fedora.getNextPID(name_space)
                page_label = book_label + '-' + page_name
                page_pid = name_space + book_pid[book_pid.find(':'):] + '-' + page_name
                page_label = unicode(page_label)
                page_object = fedora.createObject(page_pid, label = page_label)
                jp2_file_path = os.path.join(source_directory, 'images-jp2', jp2_file)
                
                #add a thumnail to the book if apropriate
                if page_name == '001':
                    #create thumbnail
                    tn_file_path = jp2_file_path + '.jpg'
                    image_magic_call = ["convert", jp2_file_path, '-compress', 'JPEG', "-thumbnail", "x100", "-gravity", "center", "-extent", "x100", tn_file_path]
                    response = subprocess.call(image_magic_call)
                    
                    #ingest thumbnail
                    #tn_file_path = os.path.join(source_directory, 'images-jp2', jp2_file)
                    tn_file_handle = open(tn_file_path, 'rb')
                    try:
                        book_object.addDataStream(u'TN', u'aTmpStr', label = u'TN',
                        mimeType = u'image/jpg', controlGroup = u'M',
                        logMessage = u'Added TN datastream.')
                        datastream = book_object['TN']
                        datastream.setContent(tn_file_handle)
                        logging.info('Added TN datastream to:' + book_pid)
                    except FedoraConnectionException:
                        logging.error('Error in adding TN datastream to:' + book_pid + '\n')
                    tn_file_handle.close()
                
                #add jp2 ds
                jp2_file_handle = open(jp2_file_path, 'rb')
                try:
                    page_object.addDataStream(u'JP2', u'aTmpStr', label=u'JP2',
                    mimeType = u'image/jp2', controlGroup = u'M',
                    logMessage = u'Added JP2 datastream.')
                    datastream = page_object['JP2']
                    datastream.setContent(jp2_file_handle)
                    logging.info('Added JP2 datastream to:' + page_pid)
                except FedoraConnectionException:
                    logging.error('Error in adding JP2 datastream to:' + page_pid + '\n')
                jp2_file_handle.close()
                
                #add tei file from tei-xml/pages, there might not be one
                #we have to call these the ocr DS to make them work nice with the viewer
                tei_file = book_name + '_TEIP5_page_' + str(int(page_name)) + '.xml'
                tei_file_path = os.path.join(source_directory, 'tei-xml/pages', tei_file)
                if os.path.isfile(tei_file_path):
                    tei_file_handle = open(tei_file_path)
                    tei_contents = tei_file_handle.read()
                    tei_file_handle.close()
                    try:
                        page_object.addDataStream(u'TEI', unicode(tei_contents, encoding = 'UTF-8'), label=u'TEI',
                        mimeType=u'application/tei+xml', controlGroup=u'M',
                        logMessage=u'Added basic tei.')
                        logging.info('Added TEI datastream to:' + page_pid)
                    except FedoraConnectionException:
                        logging.error('Error in adding TEI datastream to:' + page_pid + '\n')
                
                
                #add relationships
                objRelsExt=fedora_relationships.rels_ext(page_object, fedora_model_namespace)
                objRelsExt.addRelationship('isMemberOf', book_pid)
                objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:pageCModel')
                objRelsExt.update()
    sys.exit()