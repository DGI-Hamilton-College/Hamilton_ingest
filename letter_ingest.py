'''
Created on Sep 19, 2011
This file handles the batch ingest of the civil war collection for hamilton
@author: William Panting
'''
import logging, sys, os, ConfigParser, time#, shutil
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from islandoraUtils.metadata import fedora_relationships

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
    logFile=os.path.join(log_directory,'Hamilton_batch_letter_ingest' + time.strftime('%y_%m_%d') + '.log')
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
    
    
    '''
    do ingest
    '''
        

    #loop through the mods folder
    for mods_file in mods_files:
        if mods_file.endswith('MODS.xml'):
            
            book_name = mods_file[:mods_file.find('_')]
            
            #create a book object
            book_pid = fedora.getNextPID(u'hamilton')
            book_label=unicode(book_name)
            book_object = fedora.createObject(book_pid, label = book_label)
            
            #add mods datastream
            mods_file_path = os.path.join(source_directory, 'mods-xml', mods_file)
            mods_file_handle = open(mods_file_path)
            mods_contents = mods_file_handle.read()
            mods_file_handle.close()
            try:
                book_object.addDataStream(u'MODS', unicode(mods_contents), label=u'MODS',
                mimeType=u'text/xml', controlGroup=u'X',
                logMessage=u'Added basic mods meta data.')
                logging.info('Added MODS datastream to:'+book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding MODS datastream to:'+book_pid+'\n')
            
            #add pdf ds
            pdf_file = book_name + '.pdf'
            pdf_file_path = os.path.join(source_directory, 'images-pdf', pdf_file)
            print(pdf_file_path)
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
                mimeType=u'text/xml', controlGroup=u'M',
                logMessage=u'Added basic tei meta data.')
                logging.info('Added tei datastream to:' + book_pid)
            except FedoraConnectionException:
                logging.error('Error in adding tei datastream to:' + book_pid + '\n')
            
            #add relationships
            objRelsExt=fedora_relationships.rels_ext(book_object,fedora_relationships.rels_namespace('fedora-model','info:fedora/fedora-system:def/model#'))
            objRelsExt.addRelationship('isMemberOf','islandora:top')#this might change to a collection hamilton:cwl
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
                page_pid = fedora.getNextPID(u'hamilton')
                page_label = book_label + '_' + page_name
                page_label = unicode(page_label)
                page_object = fedora.createObject(page_pid, label = page_label)
                
                #add jp2 ds
                jp2_file_path = os.path.join(source_directory, 'images-jp2', jp2_file)
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
                tei_file = book_name + '_TEIP5_page_' + str(int(page_name)) + '.xml'
                tei_file_path = os.path.join(source_directory, 'tei-xml/pages', tei_file)
                if os.path.isfile(tei_file_path):
                    tei_file_handle = open(tei_file_path)
                    print(tei_file_handle)
                    print(tei_contents)
                    tei_contents = tei_file_handle.read()
                    tei_file_handle.close()
                    try:
                        page_object.addDataStream(u'TEI', unicode(tei_contents, encoding = 'UTF-8'), label=u'TEI',
                        mimeType=u'text/xml', controlGroup=u'M',
                        logMessage=u'Added basic tei meta data.')
                        logging.info('Added tei datastream to:' + page_pid)
                    except FedoraConnectionException:
                        logging.error('Error in adding tei datastream to:' + page_pid + '\n')
                
                
                #add relationships
                objRelsExt=fedora_relationships.rels_ext(page_object, fedora_relationships.rels_namespace('fedora-model','info:fedora/fedora-system:def/model#'))
                objRelsExt.addRelationship('isMemberOf', book_pid)#this might change to a collection hamilton:cwl
                objRelsExt.addRelationship(fedora_relationships.rels_predicate('fedora-model','hasModel'),'islandora:pageCModel')
                objRelsExt.update()
    sys.exit()